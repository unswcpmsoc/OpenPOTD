import asyncio
import io
import re
import sqlite3
from datetime import date
from datetime import datetime
from datetime import timedelta
import logging

import discord
from discord import app_commands
import schedule
from discord.ext import commands
from discord.ext.commands import BucketType, flags

import openpotd
import shared

authorised_set = set()


def authorised(ctx):
    return ctx.author.id in authorised_set


class Management(commands.Cog):

    def __init__(self, bot: openpotd.OpenPOTD):
        self.bot = bot
        self.logger = logging.getLogger('management')
        self._delete_up_to_menu = app_commands.ContextMenu(
            name='Delete Bot Messages Up To Here',
            callback=self.delete_bot_messages_up_to_here,
        )
        try:
            self.bot.tree.add_command(self._delete_up_to_menu)
        except app_commands.CommandAlreadyRegistered:
            pass
        if self.bot.config.get('posting_time'):
            try:
                schedule.every().day.at(str(self.bot.config['posting_time'])).do(self.schedule_potd)
            except schedule.ScheduleValueError as e:
                self.logger.error(f'Invalid posting_time {self.bot.config["posting_time"]!r}: {e}')
        else:
            self.logger.warning('No posting_time configured; automatic daily posting is disabled.')
        global authorised_set
        authorised = self.bot.config.get('authorised') or []
        if isinstance(authorised, int):
            authorised_set = {authorised}
        else:
            authorised_set = set(authorised)

    def cog_unload(self):
        try:
            self.bot.tree.remove_command(self._delete_up_to_menu.name, type=self._delete_up_to_menu.type)
        except Exception:
            pass

    def _filter_allowed_servers(self, servers):
        return [server for server in servers if self.bot.is_allowed_guild_id(server[0])]

    def _is_authorised_user(self, user_id: int) -> bool:
        return user_id in authorised_set

    def _has_reviewer_role(self, member: discord.abc.User, guild: discord.Guild | None) -> bool:
        if guild is None or not isinstance(member, discord.Member):
            return False

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT submission_ping_role_id FROM config WHERE server_id = ?', (guild.id,))
        row = cursor.fetchone()
        if row is None or row[0] is None:
            return False

        reviewer_role_id = int(row[0])
        return any(role.id == reviewer_role_id for role in member.roles)

    def _can_review_submission(self, member: discord.abc.User, guild: discord.Guild | None) -> bool:
        return self._is_authorised_user(member.id) or self._has_reviewer_role(member, guild)

    async def _clean_dm_impl(self, user: discord.User, lookback_minutes: int):
        dm_channel = user.dm_channel or await user.create_dm()
        cutoff = discord.utils.utcnow() - timedelta(minutes=lookback_minutes)
        deleted_count = 0
        failed_count = 0

        async for message in dm_channel.history(limit=None, after=cutoff):
            if message.author.id != self.bot.user.id:
                continue
            try:
                await message.delete()
                deleted_count += 1
            except discord.HTTPException:
                failed_count += 1

        summary = (
            f'Deleted `{deleted_count}` bot message(s) in DMs with `{user.id}` '
            f'from the last `{lookback_minutes}` minute(s).'
        )
        if failed_count > 0:
            summary += f' Failed to delete `{failed_count}` message(s).'
        return summary

    async def _review_submission_impl(
            self,
            guild_id: int,
            message_id: int,
            is_correct: bool,
            reviewer_id: int,
            reviewer_note: str | None = None):
        cursor = self.bot.db.cursor()
        cursor.execute(
            'SELECT submission_id FROM manual_submission_messages '
            'WHERE server_id = ? AND (message_id = ? OR control_message_id = ?)',
            (guild_id, message_id, message_id),
        )
        result = cursor.fetchone()
        if result is None:
            return False, 'No manual submission is linked to that message in this server.'

        interface = self.bot.get_cog('Interface')
        if interface is None:
            return False, 'Interface cog is not loaded.'

        return await interface.review_manual_submission(
            result[0],
            is_correct,
            reviewer_id,
            reviewer_note=reviewer_note,
        )

    async def _assign_roles_impl(self, season: int):
        cursor = self.bot.db.cursor()
        cursor.execute('SELECT bronze_cutoff, silver_cutoff, gold_cutoff from seasons WHERE id = ?', (season,))
        result = cursor.fetchall()

        if len(result) == 0:
            return False, 'No such season!'

        cutoffs = result[0]
        cursor.execute('SELECT server_id, bronze_role_id, silver_role_id, gold_role_id from config')
        servers = self._filter_allowed_servers(cursor.fetchall())

        cursor.execute('SELECT user_id from rankings inner join users on rankings.user_id = users.discord_id '
                       'where season_id = ? and score > ? and score < ? and users.receiving_medal_roles = ?',
                       (season, cutoffs[0], cutoffs[1], True))
        bronzes = [x[0] for x in cursor.fetchall()]

        cursor.execute('SELECT user_id from rankings inner join users on rankings.user_id = users.discord_id '
                       'where season_id = ? and score > ? and score < ? and users.receiving_medal_roles = ?',
                       (season, cutoffs[1], cutoffs[2], True))
        silvers = [x[0] for x in cursor.fetchall()]

        cursor.execute('SELECT user_id from rankings inner join users on rankings.user_id = users.discord_id '
                       'where season_id = ? and score > ? and users.receiving_medal_roles = ?',
                       (season, cutoffs[2], True))
        golds = [x[0] for x in cursor.fetchall()]
        medallers = [bronzes, silvers, golds]

        for server in servers:
            server_id = server[0]
            guild: discord.Guild = self.bot.get_guild(server_id)

            if guild is None:
                self.logger.warning(f'[{server_id}] Trying to assign roles: No such guild {server_id}')
                continue

            self_member: discord.Member = guild.get_member(self.bot.user.id)
            if not discord.Permissions.manage_roles.flag & self_member.guild_permissions.value:
                self.logger.warning(f'[{server_id}] Trying to assign roles: No permissions in guild {server_id}')
                continue

            # Clear all the bronze, silver and gold roles
            for x in (server[1], server[2], server[3]):  # Bronze, Silver, Gold role IDs
                if x is not None:
                    medal_role: discord.Role = guild.get_role(x)
                    if medal_role is None:
                        self.logger.warning(f'[{server_id}] Trying to assign roles: Guild {server_id} has no role {x}')
                        continue
                    for user in medal_role.members:
                        user: discord.Member
                        try:
                            await user.remove_roles(medal_role)
                        except Exception:
                            self.logger.warning(f'[{server_id}] Trying to assign roles: '
                                                f'Guild {server_id} missing permissions. ')

            self.logger.info(f'[{server_id}] Removed all medal roles from guild {server_id}')

            # Give roles
            for x in range(3):
                if server[x + 1] is not None:
                    medal_role: discord.Role = guild.get_role(server[x + 1])
                    if medal_role is None:
                        self.logger.warning(f'[{server_id}] No role {medal_role} ({x})')
                        continue
                    for user_id in medallers[x]:
                        if guild.get_member(user_id) is not None:
                            member: discord.Member = guild.get_member(user_id)
                            try:
                                await member.add_roles(medal_role)
                            except Exception:
                                self.logger.warning(f'[{server_id}] Trying to assign roles: '
                                                    f'Guild {server_id} missing permissions. [{user_id}]')
                            self.logger.info(f'[{server_id}] Gave {user_id} role {medal_role.name}')

        return True, 'Done!'

    async def delete_bot_messages_up_to_here(self, interaction: discord.Interaction, message: discord.Message):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            if interaction.response.is_done():
                await interaction.followup.send('You are not authorised to use this command.', ephemeral=True)
            else:
                await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        channel = message.channel
        if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            await interaction.response.send_message('This channel type is not supported.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        deleted_count = 0
        failed_count = 0
        start_obj = discord.Object(id=max(message.id - 1, 1))

        try:
            async for candidate in channel.history(limit=None, after=start_obj, oldest_first=True):
                if candidate.author.id != self.bot.user.id:
                    continue
                try:
                    await candidate.delete()
                    deleted_count += 1
                except discord.HTTPException:
                    failed_count += 1
        except discord.Forbidden:
            await interaction.followup.send('I do not have permission to read/delete messages here.', ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.followup.send('Failed while reading channel history.', ephemeral=True)
            return

        details = f'Deleted `{deleted_count}` bot message(s) from this channel up to message `{message.id}`.'
        if failed_count > 0:
            details += f' Failed to delete `{failed_count}` message(s).'
        await interaction.followup.send(details, ephemeral=True)

    def schedule_potd(self):
        asyncio.run_coroutine_threadsafe(self.advance_potd(), self.bot.loop)

    async def _post_specific_problem_impl(self, problem: shared.POTD, ping: bool):
        cursor = self.bot.db.cursor()
        cursor.execute(
            'SELECT server_id, potd_channel, ping_role_id, solved_role_id, otd_prefix, '
            'subproblem_thread_channel_id, auto_publish_news from config '
            'WHERE potd_channel IS NOT NULL'
        )
        servers = self._filter_allowed_servers(cursor.fetchall())
        posted = 0
        failed = 0

        for server in servers:
            try:
                ping_role_id = server[2] if ping else None
                await problem.post(self.bot, server[1], ping_role_id, server[5], bool(server[6]))
                posted += 1
            except Exception:
                failed += 1
                self.logger.exception(
                    f'Failed to post {shared.config_otd_label(self.bot.config)} {problem.id} in server {server[0]}.'
                )

        return posted, failed

    async def advance_potd(self):
        # Let the bot and users know we are posting the problem
        await self.bot.started_posting()

        try:
            self.logger.info(f'Advancing POTD at {datetime.now()}')
            cursor = self.bot.db.cursor()

            cursor.execute(
                'SELECT server_id, potd_channel, ping_role_id, solved_role_id, otd_prefix, '
                'subproblem_thread_channel_id, auto_publish_news from config '
                           'WHERE potd_channel IS NOT NULL')
            servers = cursor.fetchall()
            servers = self._filter_allowed_servers(servers)

            cursor.execute('SELECT problems.id, difficulty from (seasons inner join problems on seasons.running = ? '
                           'and seasons.id = problems.season and problems.date = ? ) where problems.id IS NOT NULL',
                           (True, str(date.today())))
            result = cursor.fetchall()

            cursor.execute('SELECT EXISTS (SELECT * from seasons where seasons.running = ?)', (True,))
            running_seasons_exists = cursor.fetchall()[0][0]

            # If there's no running season at all then it isn't really "running late" more like just
            # not even having a season
            if not running_seasons_exists:
                # We just do nothing since we would have already announced the
                # fact that no seasons are running.
                return

            # If there's a running season but no problem then say
            if len(result) == 0 or result[0][0] is None:
                for server in servers:
                    potd_channel = self.bot.get_channel(server[1])
                    if potd_channel is not None:
                        server_label = shared.format_otd_label(server[4], lowercase=True)
                        self.bot.loop.create_task(shared.send_with_auto_publish(
                            potd_channel,
                            f'Sorry! We are running late on the {server_label} today. ',
                            logger=self.logger,
                            log_prefix='LATE NOTICE',
                            auto_publish=bool(server[6]),
                        ))
                        self.logger.info(f'Informed server {server[0]} that there is no problem today.')
                return

            # Grab the potd
            potd_id = result[0][0]
            problem = shared.POTD(result[0][0], self.bot.db)

            for server in servers:
                # Post the problem
                try:
                    await problem.post(self.bot, server[1], server[2], server[5], bool(server[6]))
                except Exception:
                    self.logger.warning(f'Server {server[0]} channel doesn\'t exist.')

                # Remove the solved role from everyone
                role_id = server[3]
                if role_id is not None:
                    try:
                        guild = self.bot.get_guild(server[0])
                        if guild.get_role(role_id) is not None:
                            role = guild.get_role(role_id)
                            for member in role.members:
                                if member.id not in authorised_set:
                                    await member.remove_roles(role)
                    except Exception as e:
                        self.logger.warning(f'Server {server[0]}, {e}')

            # Advance the season
            cursor.execute('SELECT season FROM problems WHERE id = ?', (potd_id,))
            season_id = cursor.fetchall()[0][0]
            cursor.execute('UPDATE seasons SET latest_potd = ? WHERE id = ?', (potd_id, season_id))

            # Make the new potd publicly available
            cursor.execute('UPDATE problems SET public = ? WHERE id = ?', (True, potd_id))

            # Clear cooldowns from the previous question
            interface = self.bot.get_cog('Interface')
            if interface is not None:
                interface.cooldowns.clear()

            # Commit db
            self.bot.db.commit()

            # Log this
            self.logger.info(f'Posted {shared.config_otd_label(self.bot.config)} {potd_id}. ')

        finally:
            # Let the bot and users know we are done posting the problem
            await self.bot.finished_posting()

    @commands.command()
    @commands.check(authorised)
    async def post(self, ctx):
        await self.advance_potd()

    @app_commands.command(name='post', description='Post today\'s PoTW immediately.')
    async def post_slash(self, interaction: discord.Interaction):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.advance_potd()
        await interaction.followup.send('Posting flow completed.', ephemeral=True)

    @commands.command(name='post_problem')
    @commands.check(authorised)
    async def post_problem(self, ctx, problem: shared.POTD, ping: bool = False):
        if self.bot.posting_problem:
            await ctx.send('Posting is already in progress.')
            return

        await self.bot.started_posting()
        try:
            posted, failed = await self._post_specific_problem_impl(problem, ping=ping)
        finally:
            await self.bot.finished_posting()

        await ctx.send(
            f'Posted {shared.config_otd_label(self.bot.config)} `{problem.id}` to `{posted}` server(s). '
            f'Failed in `{failed}` server(s).'
        )

    @app_commands.command(
        name='post_problem',
        description='Post a specific problem ID now (including subproblem threads).',
    )
    @app_commands.describe(
        problem_id='Problem ID to post',
        ping='Whether to include the configured ping role when posting',
    )
    async def post_problem_slash(self, interaction: discord.Interaction, problem_id: int, ping: bool = False):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        try:
            problem = shared.POTD(problem_id, self.bot.db)
        except Exception:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        if self.bot.posting_problem:
            await interaction.response.send_message('Posting is already in progress.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.bot.started_posting()
        try:
            posted, failed = await self._post_specific_problem_impl(problem, ping=ping)
        finally:
            await self.bot.finished_posting()

        await interaction.followup.send(
            f'Posted {shared.config_otd_label(self.bot.config)} `{problem.id}` to `{posted}` server(s). '
            f'Failed in `{failed}` server(s).',
            ephemeral=True,
        )

    @commands.command()
    @commands.check(authorised)
    async def newseason(self, ctx, *, name):
        cursor = self.bot.db.cursor()
        cursor.execute('''INSERT INTO seasons (running, name) VALUES (?, ?)''', (False, name))
        self.bot.db.commit()
        cursor.execute('''SELECT LAST_INSERT_ROWID()''')
        rowid = cursor.fetchone()[0]
        await ctx.send(f'Added a new season called `{name}` with id `{rowid}`. ')
        self.logger.info(f'{ctx.author.id} added a new season called {name} with id {rowid}. ')

    @app_commands.command(name='newseason', description='Create a new season.')
    async def newseason_slash(self, interaction: discord.Interaction, name: str):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('INSERT INTO seasons (running, name) VALUES (?, ?)', (False, name))
        self.bot.db.commit()
        cursor.execute('SELECT LAST_INSERT_ROWID()')
        rowid = cursor.fetchone()[0]
        await interaction.response.send_message(
            f'Added a new season called `{name}` with id `{rowid}`.',
            ephemeral=True,
        )
        self.logger.info(f'{interaction.user.id} added a new season called {name} with id {rowid}. ')

    @commands.command()
    @commands.check(authorised)
    async def add(self, ctx, season: int, prob_date, answer: str = None, *, statement: str = None):
        parsed_answer = None
        parsed_statement = statement

        if parsed_statement is None:
            # Backward compatible parsing:
            # - If third token is an int, treat it as answer and leave statement empty.
            # - Otherwise treat it as the statement with no auto-check answer.
            if answer is not None:
                try:
                    parsed_answer = int(answer)
                    parsed_statement = None
                except (TypeError, ValueError):
                    parsed_statement = answer
                    parsed_answer = None
        else:
            if answer is not None:
                try:
                    parsed_answer = int(answer)
                except (TypeError, ValueError):
                    await ctx.send('Answer must be an integer when provided.')
                    return

        cursor = self.bot.db.cursor()
        try:
            prob_date_parsed = date.fromisoformat(prob_date)
        except ValueError:
            await ctx.send('Invalid date. Use YYYY-MM-DD.')
            return

        final_statement = (parsed_statement or '').strip()
        if not final_statement:
            final_statement = '[Main problem managed via subproblems]'

        manual_marking = parsed_answer is None
        stored_answer = 0 if parsed_answer is None else parsed_answer

        cursor.execute(
            'INSERT INTO problems ("date", season, statement, answer, manual_marking, public) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (prob_date_parsed, season, final_statement, stored_answer, manual_marking, False),
        )
        self.bot.db.commit()
        if manual_marking:
            await ctx.send(
                f'Added problem. ID: `{cursor.lastrowid}`. '
                f'No answer provided, so manual marking was enabled automatically.'
            )
        else:
            await ctx.send(f'Added problem. ID: `{cursor.lastrowid}`.')
        self.logger.info(f'{ctx.author.id} added a new problem. ')

    @commands.command(name='add_subproblem')
    @commands.check(authorised)
    async def add_subproblem(self, ctx, problem_id: int, label: str, marks: int, answer: str = None, *, statement: str = None):
        clean_label = label.strip()
        if not clean_label:
            await ctx.send('Subproblem label cannot be empty.')
            return
        if marks < 0:
            await ctx.send('Marks must be non-negative.')
            return

        parsed_answer = None
        parsed_statement = statement
        if parsed_statement is None:
            if answer is not None:
                try:
                    parsed_answer = int(answer)
                    parsed_statement = ''
                except (TypeError, ValueError):
                    parsed_statement = answer
                    parsed_answer = None
        else:
            if answer is not None:
                try:
                    parsed_answer = int(answer)
                except (TypeError, ValueError):
                    await ctx.send('Answer must be an integer when provided.')
                    return

        final_statement = (parsed_statement or '').strip()
        if not final_statement:
            final_statement = '[See attached image or context]'
        manual_marking = parsed_answer is None

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await ctx.send(f'No problem with ID `{problem_id}`.')
            return

        cursor.execute('SELECT COALESCE(MAX(order_index), 0) + 1 FROM subproblems WHERE potd_id = ?', (problem_id,))
        next_order = cursor.fetchone()[0]
        try:
            cursor.execute(
                'INSERT INTO subproblems (potd_id, label, statement, marks, answer, manual_marking, order_index, public) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (problem_id, clean_label, final_statement, marks, parsed_answer, manual_marking, next_order, True),
            )
            # If a problem has subproblems, treat main-problem direct grading as disabled.
            cursor.execute('UPDATE problems SET manual_marking = ? WHERE id = ?', (True, problem_id))
            self.bot.db.commit()
        except sqlite3.IntegrityError:
            await ctx.send(f'Subproblem label `{clean_label}` already exists for problem `{problem_id}`.')
            return

        mode = 'manual review' if manual_marking else f'auto-check (`{parsed_answer}`)'
        await ctx.send(
            f'Added subproblem `{clean_label}` with ID `{cursor.lastrowid}` to problem `{problem_id}`. Mode: {mode}.'
        )

    @app_commands.command(name='add_subproblem', description='Add a subproblem to a main problem.')
    @app_commands.describe(
        answer='Optional integer answer; omit for manual-marking subproblem',
        statement='Optional subproblem statement',
    )
    async def add_subproblem_slash(
            self,
            interaction: discord.Interaction,
            problem_id: int,
            label: str,
            marks: int,
            answer: int = None,
            statement: str = None):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        clean_label = label.strip()
        if not clean_label:
            await interaction.response.send_message('Subproblem label cannot be empty.', ephemeral=True)
            return
        if marks < 0:
            await interaction.response.send_message('Marks must be non-negative.', ephemeral=True)
            return

        final_statement = (statement or '').strip()
        if not final_statement:
            final_statement = '[See attached image or context]'
        manual_marking = answer is None

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        cursor.execute('SELECT COALESCE(MAX(order_index), 0) + 1 FROM subproblems WHERE potd_id = ?', (problem_id,))
        next_order = cursor.fetchone()[0]
        try:
            cursor.execute(
                'INSERT INTO subproblems (potd_id, label, statement, marks, answer, manual_marking, order_index, public) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (problem_id, clean_label, final_statement, marks, answer, manual_marking, next_order, True),
            )
            cursor.execute('UPDATE problems SET manual_marking = ? WHERE id = ?', (True, problem_id))
            self.bot.db.commit()
        except sqlite3.IntegrityError:
            await interaction.response.send_message(
                f'Subproblem label `{clean_label}` already exists for problem `{problem_id}`.',
                ephemeral=True,
            )
            return

        mode = 'manual review' if manual_marking else f'auto-check (`{answer}`)'
        await interaction.response.send_message(
            f'Added subproblem `{clean_label}` with ID `{cursor.lastrowid}` to problem `{problem_id}`. Mode: {mode}.',
            ephemeral=True,
        )

    @commands.command(name='list_subproblems')
    @commands.check(authorised)
    async def list_subproblems(self, ctx, problem_id: int):
        cursor = self.bot.db.cursor()
        cursor.execute(
            'SELECT id, label, marks, answer, manual_marking FROM subproblems '
            'WHERE potd_id = ? ORDER BY order_index ASC, id ASC',
            (problem_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            await ctx.send('No subproblems configured for that problem.')
            return
        lines = []
        for row in rows:
            mode = 'manual' if bool(row[4]) else f'auto ({row[3] if row[3] is not None else "unset"})'
            lines.append(f'`{row[0]}` - `{row[1]}` (`{row[2]}` marks) - {mode}')
        formatted = '\n'.join(lines)
        await ctx.send(f'Subproblems for `{problem_id}`:\n{formatted}')

    @app_commands.command(name='list_subproblems', description='List subproblems for a main problem.')
    async def list_subproblems_slash(self, interaction: discord.Interaction, problem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute(
            'SELECT id, label, marks, answer, manual_marking FROM subproblems '
            'WHERE potd_id = ? ORDER BY order_index ASC, id ASC',
            (problem_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            await interaction.response.send_message('No subproblems configured for that problem.', ephemeral=True)
            return
        lines = []
        for row in rows:
            mode = 'manual' if bool(row[4]) else f'auto ({row[3] if row[3] is not None else "unset"})'
            lines.append(f'`{row[0]}` - `{row[1]}` (`{row[2]}` marks) - {mode}')
        formatted = '\n'.join(lines)
        await interaction.response.send_message(f'Subproblems for `{problem_id}`:\n{formatted}', ephemeral=True)

    @app_commands.command(name='add', description='Add a new PoTW problem.')
    @app_commands.describe(
        prob_date='Date in YYYY-MM-DD format',
        answer='Optional integer answer; omit for subproblem/manual-marking problems',
        statement='Optional main problem statement',
    )
    async def add_slash(
            self,
            interaction: discord.Interaction,
            season: int,
            prob_date: str,
            answer: int = None,
            statement: str = None):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        try:
            prob_date_parsed = date.fromisoformat(prob_date)
        except ValueError:
            await interaction.response.send_message('Invalid date. Use YYYY-MM-DD.', ephemeral=True)
            return

        final_statement = (statement or '').strip()
        if not final_statement:
            final_statement = '[Main problem managed via subproblems]'

        manual_marking = answer is None
        stored_answer = 0 if answer is None else answer

        cursor = self.bot.db.cursor()
        cursor.execute(
            'INSERT INTO problems ("date", season, statement, answer, manual_marking, public) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (prob_date_parsed, season, final_statement, stored_answer, manual_marking, False),
        )
        self.bot.db.commit()
        if manual_marking:
            await interaction.response.send_message(
                f'Added problem. ID: `{cursor.lastrowid}`. '
                f'No answer provided, so manual marking was enabled automatically.',
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(f'Added problem. ID: `{cursor.lastrowid}`.', ephemeral=True)
        self.logger.info(f'{interaction.user.id} added a new problem. ')

    def _subproblem_exists(self, subproblem_id: int) -> bool:
        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM subproblems WHERE id = ?)', (subproblem_id,))
        return bool(cursor.fetchone()[0])

    def _fetch_linked_image_ids(self, table_name: str, foreign_key: str, target_id: int):
        cursor = self.bot.db.cursor()
        cursor.execute(
            f'SELECT id FROM {table_name} WHERE {foreign_key} = ? ORDER BY id ASC',
            (target_id,),
        )
        return [row[0] for row in cursor.fetchall()]

    def _remove_linked_image_by_index(
            self,
            table_name: str,
            foreign_key: str,
            target_id: int,
            index: int = None):
        image_ids = self._fetch_linked_image_ids(table_name, foreign_key, target_id)
        if len(image_ids) == 0:
            return False, 'No linked images found.'

        if index is None:
            remove_position = len(image_ids)  # default to last image
        else:
            remove_position = index
            if remove_position < 1 or remove_position > len(image_ids):
                return False, f'Image index must be between 1 and {len(image_ids)}.'

        image_row_id = image_ids[remove_position - 1]
        cursor = self.bot.db.cursor()
        cursor.execute(f'DELETE FROM {table_name} WHERE id = ?', (image_row_id,))
        self.bot.db.commit()
        return True, f'Removed image `{remove_position}` (row `{image_row_id}`). `{len(image_ids) - 1}` remaining.'

    @commands.command()
    @commands.check(authorised)
    async def linkimg(self, ctx, problem: shared.POTD):
        potd = problem.id
        if len(ctx.message.attachments) < 1:
            await ctx.send("No attached file. ")
            return
        else:
            save_path = io.BytesIO()
            await ctx.message.attachments[0].save(save_path)
            cursor = self.bot.db.cursor()
            cursor.execute('''INSERT INTO images (potd_id, image) VALUES (?, ?)''',
                           (potd, sqlite3.Binary(save_path.getbuffer())))
            self.bot.db.commit()
            save_path.close()

    @commands.command(name='link_subimg')
    @commands.check(authorised)
    async def link_subimg(self, ctx, subproblem_id: int):
        if len(ctx.message.attachments) < 1:
            await ctx.send('No attached file.')
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM subproblems WHERE id = ?)', (subproblem_id,))
        if not cursor.fetchone()[0]:
            await ctx.send(f'No subproblem with ID `{subproblem_id}`.')
            return

        save_path = io.BytesIO()
        await ctx.message.attachments[0].save(save_path)
        cursor.execute(
            'INSERT INTO subproblem_images (subproblem_id, image) VALUES (?, ?)',
            (subproblem_id, sqlite3.Binary(save_path.getbuffer())),
        )
        self.bot.db.commit()
        save_path.close()
        await ctx.send('Linked image to subproblem.')

    @app_commands.command(name='link_subimg', description='Attach an image to a subproblem.')
    async def link_subimg_slash(self, interaction: discord.Interaction, subproblem_id: int, image: discord.Attachment):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM subproblems WHERE id = ?)', (subproblem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No subproblem with ID `{subproblem_id}`.', ephemeral=True)
            return

        content = await image.read()
        cursor.execute(
            'INSERT INTO subproblem_images (subproblem_id, image) VALUES (?, ?)',
            (subproblem_id, sqlite3.Binary(content)),
        )
        self.bot.db.commit()
        await interaction.response.send_message('Linked image to subproblem.', ephemeral=True)

    @app_commands.command(name='linkimg', description='Attach an image to a problem.')
    async def linkimg_slash(self, interaction: discord.Interaction, problem_id: int, image: discord.Attachment):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        content = await image.read()
        cursor.execute('INSERT INTO images (potd_id, image) VALUES (?, ?)', (problem_id, sqlite3.Binary(content)))
        self.bot.db.commit()
        await interaction.response.send_message(f'Linked image to problem `{problem_id}`.', ephemeral=True)

    @commands.command(name='list_imgs')
    @commands.check(authorised)
    async def list_imgs(self, ctx, *, problem: shared.POTD):
        image_ids = self._fetch_linked_image_ids('images', 'potd_id', problem.id)
        if len(image_ids) == 0:
            await ctx.send(f'Problem `{problem.id}` has no linked images.')
            return

        lines = [f'`{idx}` -> row `{row_id}`' for idx, row_id in enumerate(image_ids, start=1)]
        await ctx.send(f'Problem `{problem.id}` image list ({len(image_ids)} total):\n' + '\n'.join(lines))

    @app_commands.command(name='list_imgs', description='List linked images for a problem.')
    async def list_imgs_slash(self, interaction: discord.Interaction, problem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        image_ids = self._fetch_linked_image_ids('images', 'potd_id', problem_id)
        if len(image_ids) == 0:
            await interaction.response.send_message(f'Problem `{problem_id}` has no linked images.', ephemeral=True)
            return

        lines = [f'`{idx}` -> row `{row_id}`' for idx, row_id in enumerate(image_ids, start=1)]
        await interaction.response.send_message(
            f'Problem `{problem_id}` image list ({len(image_ids)} total):\n' + '\n'.join(lines),
            ephemeral=True,
        )

    @commands.command(name='remove_img')
    @commands.check(authorised)
    async def remove_img(self, ctx, problem: shared.POTD, index: int = None):
        ok, details = self._remove_linked_image_by_index('images', 'potd_id', problem.id, index)
        await ctx.send(details if ok else f'Could not remove image: {details}')

    @app_commands.command(name='remove_img', description='Remove one linked image from a problem.')
    @app_commands.describe(
        problem_id='Problem ID',
        index='1-based image index; omit to remove the last image',
    )
    async def remove_img_slash(self, interaction: discord.Interaction, problem_id: int, index: int = None):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        ok, details = self._remove_linked_image_by_index('images', 'potd_id', problem_id, index)
        await interaction.response.send_message(
            details if ok else f'Could not remove image: {details}',
            ephemeral=True,
        )

    @commands.command(name='list_subimgs')
    @commands.check(authorised)
    async def list_subimgs(self, ctx, subproblem_id: int):
        if not self._subproblem_exists(subproblem_id):
            await ctx.send(f'No subproblem with ID `{subproblem_id}`.')
            return

        image_ids = self._fetch_linked_image_ids('subproblem_images', 'subproblem_id', subproblem_id)
        if len(image_ids) == 0:
            await ctx.send(f'Subproblem `{subproblem_id}` has no linked images.')
            return

        lines = [f'`{idx}` -> row `{row_id}`' for idx, row_id in enumerate(image_ids, start=1)]
        await ctx.send(f'Subproblem `{subproblem_id}` image list ({len(image_ids)} total):\n' + '\n'.join(lines))

    @app_commands.command(name='list_subimgs', description='List linked images for a subproblem.')
    async def list_subimgs_slash(self, interaction: discord.Interaction, subproblem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        if not self._subproblem_exists(subproblem_id):
            await interaction.response.send_message(f'No subproblem with ID `{subproblem_id}`.', ephemeral=True)
            return

        image_ids = self._fetch_linked_image_ids('subproblem_images', 'subproblem_id', subproblem_id)
        if len(image_ids) == 0:
            await interaction.response.send_message(
                f'Subproblem `{subproblem_id}` has no linked images.',
                ephemeral=True,
            )
            return

        lines = [f'`{idx}` -> row `{row_id}`' for idx, row_id in enumerate(image_ids, start=1)]
        await interaction.response.send_message(
            f'Subproblem `{subproblem_id}` image list ({len(image_ids)} total):\n' + '\n'.join(lines),
            ephemeral=True,
        )

    @commands.command(name='remove_subimg')
    @commands.check(authorised)
    async def remove_subimg(self, ctx, subproblem_id: int, index: int = None):
        if not self._subproblem_exists(subproblem_id):
            await ctx.send(f'No subproblem with ID `{subproblem_id}`.')
            return
        ok, details = self._remove_linked_image_by_index('subproblem_images', 'subproblem_id', subproblem_id, index)
        await ctx.send(details if ok else f'Could not remove image: {details}')

    @app_commands.command(name='remove_subimg', description='Remove one linked image from a subproblem.')
    @app_commands.describe(
        subproblem_id='Subproblem ID',
        index='1-based image index; omit to remove the last image',
    )
    async def remove_subimg_slash(self, interaction: discord.Interaction, subproblem_id: int, index: int = None):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        if not self._subproblem_exists(subproblem_id):
            await interaction.response.send_message(f'No subproblem with ID `{subproblem_id}`.', ephemeral=True)
            return

        ok, details = self._remove_linked_image_by_index('subproblem_images', 'subproblem_id', subproblem_id, index)
        await interaction.response.send_message(
            details if ok else f'Could not remove image: {details}',
            ephemeral=True,
        )

    @commands.command(name='clear_subimgs')
    @commands.check(authorised)
    async def clear_subimgs(self, ctx, subproblem_id: int):
        if not self._subproblem_exists(subproblem_id):
            await ctx.send(f'No subproblem with ID `{subproblem_id}`.')
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT COUNT(1) FROM subproblem_images WHERE subproblem_id = ?', (subproblem_id,))
        existing = cursor.fetchone()[0]
        cursor.execute('DELETE FROM subproblem_images WHERE subproblem_id = ?', (subproblem_id,))
        self.bot.db.commit()
        await ctx.send(f'Cleared `{existing}` image(s) from subproblem `{subproblem_id}`.')

    @app_commands.command(name='clear_subimgs', description='Remove all linked images from a subproblem.')
    async def clear_subimgs_slash(self, interaction: discord.Interaction, subproblem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        if not self._subproblem_exists(subproblem_id):
            await interaction.response.send_message(f'No subproblem with ID `{subproblem_id}`.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT COUNT(1) FROM subproblem_images WHERE subproblem_id = ?', (subproblem_id,))
        existing = cursor.fetchone()[0]
        cursor.execute('DELETE FROM subproblem_images WHERE subproblem_id = ?', (subproblem_id,))
        self.bot.db.commit()
        await interaction.response.send_message(
            f'Cleared `{existing}` image(s) from subproblem `{subproblem_id}`.',
            ephemeral=True,
        )

    @commands.command(name='link_thread')
    @commands.check(authorised)
    async def link_thread(self, ctx, subproblem_id: int, thread: discord.Thread = None):
        if ctx.guild is None:
            await ctx.send('This command can only be used in a server.')
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM subproblems WHERE id = ?)', (subproblem_id,))
        if not cursor.fetchone()[0]:
            await ctx.send(f'No subproblem with ID `{subproblem_id}`.')
            return

        target_thread = thread
        if target_thread is None and isinstance(ctx.channel, discord.Thread):
            target_thread = ctx.channel

        if target_thread is None:
            await ctx.send('Provide a thread, or run this command inside the thread to link it.')
            return

        if target_thread.guild.id != ctx.guild.id:
            await ctx.send('Please select a thread in this server.')
            return

        parent_channel_id = target_thread.parent_id if target_thread.parent_id is not None else target_thread.id
        cursor.execute(
            'DELETE FROM subproblem_threads WHERE subproblem_id = ? AND server_id = ?',
            (subproblem_id, ctx.guild.id),
        )
        cursor.execute(
            'INSERT INTO subproblem_threads (subproblem_id, server_id, channel_id, message_id, thread_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (subproblem_id, ctx.guild.id, parent_channel_id, 0, target_thread.id),
        )
        self.bot.db.commit()
        await ctx.send(
            f'Linked thread <#{target_thread.id}> to subproblem `{subproblem_id}`.'
        )

    @app_commands.command(name='link_thread', description='Link a discussion thread to a subproblem.')
    async def link_thread_slash(
            self,
            interaction: discord.Interaction,
            subproblem_id: int,
            thread: discord.Thread = None):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return
        if interaction.guild is None:
            await interaction.response.send_message('This command can only be used in a server.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM subproblems WHERE id = ?)', (subproblem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No subproblem with ID `{subproblem_id}`.', ephemeral=True)
            return

        target_thread = thread
        if target_thread is None and isinstance(interaction.channel, discord.Thread):
            target_thread = interaction.channel

        if target_thread is None:
            await interaction.response.send_message(
                'Provide a thread, or run this command inside the thread to link.',
                ephemeral=True,
            )
            return

        if target_thread.guild.id != interaction.guild.id:
            await interaction.response.send_message('Please select a thread in this server.', ephemeral=True)
            return

        parent_channel_id = target_thread.parent_id if target_thread.parent_id is not None else target_thread.id
        cursor.execute(
            'DELETE FROM subproblem_threads WHERE subproblem_id = ? AND server_id = ?',
            (subproblem_id, interaction.guild.id),
        )
        cursor.execute(
            'INSERT INTO subproblem_threads (subproblem_id, server_id, channel_id, message_id, thread_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (subproblem_id, interaction.guild.id, parent_channel_id, 0, target_thread.id),
        )
        self.bot.db.commit()
        await interaction.response.send_message(
            f'Linked thread <#{target_thread.id}> to subproblem `{subproblem_id}`.',
            ephemeral=True,
        )

    @commands.command()
    @commands.check(authorised)
    async def showpotd(self, ctx, *, problem: shared.POTD):
        """Note: this is the admin version of the command so all problems are visible. """

        otd_label = shared.config_otd_label(self.bot.config)
        images = problem.images
        if len(images) == 0:
            await ctx.send(f'{otd_label} {problem.id} of {problem.date} has no picture '
                           f'attached. ')
        else:
            await ctx.send(f'{otd_label} {problem.id} of {problem.date}',
                           file=discord.File(io.BytesIO(images[0]),
                                             filename=f'POTD-{problem.id}-0.png'))
            for i in range(1, len(images)):
                await ctx.send(file=discord.File(io.BytesIO(images[i]), filename=f'POTD-{problem.id}-{i}.png'))

    @app_commands.command(name='showpotd', description='Show a problem and its images.')
    async def showpotd_slash(self, interaction: discord.Interaction, problem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        try:
            problem = shared.POTD(problem_id, self.bot.db)
        except Exception:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        otd_label = shared.config_otd_label(self.bot.config)
        images = problem.images
        if len(images) == 0:
            await interaction.followup.send(
                f'{otd_label} {problem.id} of {problem.date} has no picture attached.',
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f'{otd_label} {problem.id} of {problem.date}',
            file=discord.File(io.BytesIO(images[0]), filename=f'POTD-{problem.id}-0.png'),
            ephemeral=True,
        )
        for i in range(1, len(images)):
            await interaction.followup.send(
                file=discord.File(io.BytesIO(images[i]), filename=f'POTD-{problem.id}-{i}.png'),
                ephemeral=True,
            )

    class UpdateFlags(commands.FlagConverter, delimiter=' ', prefix='--'):
        date: str = None
        season: int = None
        statement: str = None
        difficulty: int = None
        answer: int = None
        manual_marking: bool = None
        public: bool = None
        source: str = None
    @commands.command()
    @commands.check(authorised)
    async def update(self, ctx, problem: shared.POTD, *, flags:UpdateFlags):
        potd = problem.id
        cursor = self.bot.db.cursor()
        if not flags.date is None and not bool(re.match(r'\d\d\d\d-\d\d-\d\d', flags.date)):
            await ctx.send('Invalid date (specify yyyy-mm-dd)')
            return

        for param in vars(flags):
            if vars(flags)[param] is not None:
                cursor.execute(f'UPDATE problems SET {param} = ? WHERE id = ?', (vars(flags)[param], potd))
        self.bot.db.commit()
        await ctx.send(f'Updated {shared.config_otd_label(self.bot.config, lowercase=True)}. ')

    @commands.command(name='pinfo')
    @commands.check(authorised)
    async def info(self, ctx, problem: shared.POTD):
        info = problem.info()
        embed = discord.Embed(title=f'{shared.config_otd_label(self.bot.config)} {problem.id}')
        for i in range(len(info)):
            embed.add_field(name=info[i][0], value=f'`{info[i][1]}`', inline=False)
        await ctx.send(embed=embed)

    @app_commands.command(name='pinfo', description='Show internal info for a problem.')
    async def info_slash(self, interaction: discord.Interaction, problem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        try:
            problem = shared.POTD(problem_id, self.bot.db)
        except Exception:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        info = problem.info()
        embed = discord.Embed(title=f'{shared.config_otd_label(self.bot.config)} {problem.id}')
        for i in range(len(info)):
            embed.add_field(name=info[i][0], value=f'`{info[i][1]}`', inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def manual_marking(self, ctx, problem: shared.POTD, enabled: bool):
        cursor = self.bot.db.cursor()
        cursor.execute('UPDATE problems SET manual_marking = ? WHERE id = ?', (enabled, problem.id))
        self.bot.db.commit()
        mode = 'enabled' if enabled else 'disabled'
        await ctx.send(f'Manual marking {mode} for {shared.config_otd_label(self.bot.config)} {problem.id}.')

    @app_commands.command(name='manual_marking', description='Enable or disable manual marking for a problem ID.')
    async def manual_marking_slash(self, interaction: discord.Interaction, problem_id: int, enabled: bool):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        cursor.execute('UPDATE problems SET manual_marking = ? WHERE id = ?', (enabled, problem_id))
        self.bot.db.commit()
        mode = 'enabled' if enabled else 'disabled'
        await interaction.response.send_message(
            f'Manual marking {mode} for {shared.config_otd_label(self.bot.config)} {problem_id}.',
            ephemeral=True,
        )

    @commands.command(name='review_submission')
    @commands.guild_only()
    async def review_submission(self, ctx, decision: str, *extra):
        if not self._can_review_submission(ctx.author, ctx.guild):
            await ctx.send('You are not authorised to use this command.')
            return

        message_id = None
        note = None
        if extra:
            first = extra[0].strip()
            if first.isdecimal():
                message_id = int(first)
                if len(extra) > 1:
                    note = ' '.join(extra[1:]).strip() or None
            else:
                note = ' '.join(extra).strip() or None

        if message_id is None and ctx.message.reference is not None:
            message_id = ctx.message.reference.message_id

        if message_id is None:
            await ctx.send('Reply to a mirrored submission message or provide its message ID.')
            return

        decision_lc = decision.strip().lower()
        if decision_lc in ('correct', 'approve', 'approved', 'yes', 'y', 'true', '1'):
            is_correct = True
        elif decision_lc in ('incorrect', 'reject', 'rejected', 'no', 'n', 'false', '0'):
            is_correct = False
        else:
            await ctx.send('Decision must be one of: correct/incorrect.')
            return

        success, details = await self._review_submission_impl(
            ctx.guild.id,
            message_id,
            is_correct,
            ctx.author.id,
            reviewer_note=note,
        )
        await ctx.send(details)

    @app_commands.command(name='review_submission', description='Review a mirrored manual submission.')
    @app_commands.describe(
        decision='Use correct/incorrect',
        message_id='Mirrored submission message ID (optional if run from context menu)',
        note='Optional message to send to the submitter via DM',
    )
    async def review_submission_slash(
            self,
            interaction: discord.Interaction,
            decision: str,
            message_id: int,
            note: str = None):
        if not interaction.guild or not interaction.user:
            await interaction.response.send_message('This command can only be used in a server.', ephemeral=True)
            return
        if not self._can_review_submission(interaction.user, interaction.guild):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        decision_lc = decision.strip().lower()
        if decision_lc in ('correct', 'approve', 'approved', 'yes', 'y', 'true', '1'):
            is_correct = True
        elif decision_lc in ('incorrect', 'reject', 'rejected', 'no', 'n', 'false', '0'):
            is_correct = False
        else:
            await interaction.response.send_message('Decision must be one of: correct/incorrect.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        _, details = await self._review_submission_impl(
            interaction.guild.id,
            message_id,
            is_correct,
            interaction.user.id,
            reviewer_note=(note.strip() or None) if note is not None else None,
        )
        await interaction.followup.send(details, ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def start_season(self, ctx, season: int):
        cursor = self.bot.db.cursor()
        cursor.execute('SELECT running from seasons where seasons.id = ?', (season,))
        result = cursor.fetchall()

        if len(result) == 0:
            await ctx.send(f'No season with id {season}.')
            return

        running = result[0][0]
        if not running:
            cursor.execute('UPDATE seasons SET running = ? where seasons.id = ?', (True, season))
            self.bot.db.commit()
            self.logger.info(f'Started season with id {season}. ')
        else:
            await ctx.send(f'Season {season} already running!')

    @app_commands.command(name='start_season', description='Start a season.')
    async def start_season_slash(self, interaction: discord.Interaction, season: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT running from seasons where seasons.id = ?', (season,))
        result = cursor.fetchall()
        if len(result) == 0:
            await interaction.response.send_message(f'No season with id {season}.', ephemeral=True)
            return

        running = result[0][0]
        if not running:
            cursor.execute('UPDATE seasons SET running = ? where seasons.id = ?', (True, season))
            self.bot.db.commit()
            self.logger.info(f'Started season with id {season}. ')
            await interaction.response.send_message(f'Started season `{season}`.', ephemeral=True)
        else:
            await interaction.response.send_message(f'Season `{season}` already running!', ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def end_season(self, ctx, season: int):
        cursor = self.bot.db.cursor()
        cursor.execute('SELECT running from seasons where seasons.id = ?', (season,))
        result = cursor.fetchall()

        if len(result) == 0:
            await ctx.send(f'No season with id {season}.')
            return

        running = result[0][0]
        if running:
            cursor.execute('UPDATE seasons SET running = ? where seasons.id = ?', (False, season))
            self.bot.db.commit()
            self.logger.info(f'Ended season with id {season}. ')
        else:
            await ctx.send(f'Season {season} already stopped!')

    @app_commands.command(name='end_season', description='Stop a running season.')
    async def end_season_slash(self, interaction: discord.Interaction, season: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT running from seasons where seasons.id = ?', (season,))
        result = cursor.fetchall()
        if len(result) == 0:
            await interaction.response.send_message(f'No season with id {season}.', ephemeral=True)
            return

        running = result[0][0]
        if running:
            cursor.execute('UPDATE seasons SET running = ? where seasons.id = ?', (False, season))
            self.bot.db.commit()
            self.logger.info(f'Ended season with id {season}. ')
            await interaction.response.send_message(f'Ended season `{season}`.', ephemeral=True)
        else:
            await interaction.response.send_message(f'Season `{season}` already stopped!', ephemeral=True)

    @commands.command()
    @commands.is_owner()
    async def execute_sql(self, ctx, *, sql):
        cursor = self.bot.db.cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            await ctx.send(e)
        await ctx.send(str(cursor.fetchall()))

    @commands.command()
    @commands.is_owner()
    async def init_nicks(self, ctx):
        cursor = self.bot.db.cursor()
        cursor.execute('SELECT discord_id from users where nickname is NULL')
        users_to_check = [x[0] for x in cursor.fetchall()]

        to_update = []
        for user_id in users_to_check:
            user: discord.User = self.bot.get_user(user_id)
            if user is not None:
                to_update.append((user.display_name, user_id))
            else:
                to_update.append(('Unknown', user_id))

        cursor.executemany('UPDATE users SET nickname = ? where discord_id = ?', to_update)
        self.bot.db.commit()
        await ctx.send('Done!')

    @commands.command()
    @commands.check(authorised)
    async def announce(self, ctx, *, message: commands.clean_content):
        cursor = self.bot.db.cursor()
        cursor.execute(
            'SELECT server_id, potd_channel, auto_publish_news FROM config WHERE potd_channel IS NOT NULL'
        )
        channels = self._filter_allowed_servers(cursor.fetchall())
        self.logger.info(f"[ANNOUNCE] Announcement created by {ctx.message.author.id}")

        for _, channel_id, auto_publish_news in channels:
            channel: discord.TextChannel = self.bot.get_channel(channel_id)
            if channel is not None:
                try:
                    await shared.send_with_auto_publish(
                        channel,
                        message,
                        logger=self.logger,
                        log_prefix='ANNOUNCE',
                        auto_publish=bool(auto_publish_news),
                    )
                except Exception as e:
                    self.logger.warning(f"[ANNOUNCE] Can't send messages in {channel_id}")

    @app_commands.command(name='announce', description='Send an announcement to all configured PoTW channels.')
    async def announce_slash(self, interaction: discord.Interaction, message: str):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        cursor = self.bot.db.cursor()
        cursor.execute(
            'SELECT server_id, potd_channel, auto_publish_news FROM config WHERE potd_channel IS NOT NULL'
        )
        channels = self._filter_allowed_servers(cursor.fetchall())
        self.logger.info(f"[ANNOUNCE] Announcement created by {interaction.user.id}")

        sent = 0
        for _, channel_id, auto_publish_news in channels:
            channel: discord.TextChannel = self.bot.get_channel(channel_id)
            if channel is not None:
                try:
                    await shared.send_with_auto_publish(
                        channel,
                        message,
                        logger=self.logger,
                        log_prefix='ANNOUNCE',
                        auto_publish=bool(auto_publish_news),
                    )
                    sent += 1
                except Exception:
                    self.logger.warning(f"[ANNOUNCE] Can't send messages in {channel_id}")

        await interaction.followup.send(f'Announcement sent to `{sent}` channel(s).', ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def set_cutoffs(self, ctx, season: int, bronze: int, silver: int, gold: int):
        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (select 1 from seasons where id = ?)', (season,))
        season_exists = cursor.fetchall()[0][0]
        if season_exists:
            cursor.execute('UPDATE seasons SET bronze_cutoff = ?, silver_cutoff = ?, gold_cutoff = ? WHERE id = ?',
                           (bronze, silver, gold, season))
            self.bot.db.commit()
            await ctx.send('Done!')
        else:
            await ctx.send('No season with that ID!')

    @app_commands.command(name='set_cutoffs', description='Set bronze/silver/gold cutoffs for a season.')
    async def set_cutoffs_slash(self, interaction: discord.Interaction, season: int, bronze: int, silver: int, gold: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (select 1 from seasons where id = ?)', (season,))
        season_exists = cursor.fetchall()[0][0]
        if season_exists:
            cursor.execute(
                'UPDATE seasons SET bronze_cutoff = ?, silver_cutoff = ?, gold_cutoff = ? WHERE id = ?',
                (bronze, silver, gold, season),
            )
            self.bot.db.commit()
            await interaction.response.send_message('Done!', ephemeral=True)
        else:
            await interaction.response.send_message('No season with that ID!', ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def assign_roles(self, ctx, season: int):
        _, message = await self._assign_roles_impl(season)
        await ctx.send(message)

    @app_commands.command(name='assign_roles', description='Assign medal roles based on season rankings.')
    async def assign_roles_slash(self, interaction: discord.Interaction, season: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        _, message = await self._assign_roles_impl(season)
        await interaction.followup.send(message, ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def clear_imgs(self, ctx, *, problem: shared.POTD):
        cursor = self.bot.db.cursor()
        cursor.execute('DELETE FROM images WHERE potd_id = ?', (problem.id,))
        self.bot.db.commit()

        await ctx.send('Cleared images!')

    @app_commands.command(name='clear_imgs', description='Remove all images linked to a problem.')
    async def clear_imgs_slash(self, interaction: discord.Interaction, problem_id: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        cursor = self.bot.db.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM problems WHERE id = ?)', (problem_id,))
        if not cursor.fetchone()[0]:
            await interaction.response.send_message(f'No problem with ID `{problem_id}`.', ephemeral=True)
            return

        cursor.execute('DELETE FROM images WHERE potd_id = ?', (problem_id,))
        self.bot.db.commit()
        await interaction.response.send_message('Cleared images!', ephemeral=True)

    @commands.command()
    @commands.check(authorised)
    async def force_update(self, ctx, *, season: int):
        try:
            self.bot.get_cog('Interface').update_rankings(season)
        except Exception as e:
            await ctx.send(e)

        await ctx.send('Done!')

    @app_commands.command(name='force_update', description='Recalculate rankings for a season.')
    async def force_update_slash(self, interaction: discord.Interaction, season: int):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        interface = self.bot.get_cog('Interface')
        if interface is None:
            await interaction.response.send_message('Interface cog is not loaded.', ephemeral=True)
            return

        try:
            interface.update_rankings(season)
        except Exception as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        await interaction.response.send_message('Done!', ephemeral=True)

    @commands.command(name='reset_local_db')
    @commands.check(authorised)
    async def reset_local_db(self, ctx, confirm: str = None):
        if not self.bot.config.get('allow_local_db_reset', False):
            await ctx.send('Local DB reset is disabled. Set `allow_local_db_reset: true` in local config to enable it.')
            return
        if confirm != 'RESET':
            await ctx.send('Refusing to reset DB. Re-run with: `%reset_local_db RESET`')
            return
        if self.bot.posting_problem:
            await ctx.send('Cannot reset DB while posting is in progress.')
            return

        try:
            self.bot.reset_database_for_local_testing()
            interface = self.bot.get_cog('Interface')
            if interface is not None:
                interface.cooldowns.clear()
                if hasattr(interface, 'pending_subproblem_prompts'):
                    interface.pending_subproblem_prompts.clear()
        except Exception as e:
            await ctx.send(f'Failed to reset database: `{e}`')
            return

        await ctx.send('Local database reset complete. Re-run server setup/init commands before testing.')

    @app_commands.command(
        name='reset_local_db',
        description='Reset the local SQLite database for testing (requires local config flag).',
    )
    async def reset_local_db_slash(self, interaction: discord.Interaction, confirm: str):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return
        if not self.bot.config.get('allow_local_db_reset', False):
            await interaction.response.send_message(
                'Local DB reset is disabled. Set `allow_local_db_reset: true` in local config to enable it.',
                ephemeral=True,
            )
            return
        if confirm != 'RESET':
            await interaction.response.send_message(
                'Refusing to reset DB. Pass `RESET` exactly in the confirm field.',
                ephemeral=True,
            )
            return
        if self.bot.posting_problem:
            await interaction.response.send_message('Cannot reset DB while posting is in progress.', ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            self.bot.reset_database_for_local_testing()
            interface = self.bot.get_cog('Interface')
            if interface is not None:
                interface.cooldowns.clear()
                if hasattr(interface, 'pending_subproblem_prompts'):
                    interface.pending_subproblem_prompts.clear()
        except Exception as e:
            await interaction.followup.send(f'Failed to reset database: `{e}`', ephemeral=True)
            return

        await interaction.followup.send(
            'Local database reset complete. Re-run server setup/init commands before testing.',
            ephemeral=True,
        )

    @commands.command()
    @commands.check(authorised)
    async def change_answer(self, ctx, problem: shared.POTD, new_answer: int):
        # Get all attempts and current solves
        cursor = self.bot.db.cursor()

        cursor.execute(
            'SELECT user_id, submission from attempts where potd_id = ? and official = ? order by submit_time',
            (problem.id, True))
        attempts = cursor.fetchall()

        # Put attempts into a dictionary instead of a list
        attempts_dict = {}
        for attempt in attempts:
            if attempt[0] in attempts_dict:
                attempts_dict[attempt[0]].append(attempt[1])
            else:
                attempts_dict[attempt[0]] = [attempt[1]]

        cursor.execute('SELECT user, num_attempts from solves where problem_id = ? and official = ?',
                       (problem.id, True))
        solves = cursor.fetchall()
        # Same with solves
        solves_dict = {}
        for solve in solves:
            if solve[0] in solves_dict:
                solves_dict[solve[0]].append(solve[1])
            else:
                solves_dict[solve[0]] = [solve[1]]

        # See who actually solved it (with new answer)
        new_solves = {}
        for user in attempts_dict:
            if new_answer in attempts_dict[user]:
                # Find the place where they first submitted the right answer
                new_solves[user] = attempts_dict[user].index(new_answer) + 1

        # Make sets
        new_solved_set = set((i for i in new_solves))
        old_solved_set = set((i for i in solves_dict))

        submitted_new_only = new_solved_set - old_solved_set
        submitted_both_ans = new_solved_set.intersection(old_solved_set)
        submitted_old_only = old_solved_set - new_solved_set

        # DM people whom the change relates to
        for user in submitted_new_only:
            try:
                await self.bot.get_user(user).send(f'The answer {new_answer} that you submitted on attempt '
                                                   f'{new_solves[user]} is actually correct. ')
                self.logger.info(
                    f'[CHANGE ANS] [SUBMITTED NEW ONLY] User {user} solved after {new_solves[user]} attempts')
            except Exception as e:
                self.logger.warning(f'[CHANGE ANS] [SUBMITTED NEW ONLY] User {user} Exception when DMing {e}')

        for user in submitted_both_ans:
            try:
                await self.bot.get_user(user).send(
                    f'The answer has changed; the answer {new_answer} that you submitted on attempt '
                    f'{new_solves[user]} is actually correct. ')
                self.logger.info(
                    f'[CHANGE ANS] [SUBMITTED BOTH ANS] User {user} solved after {new_solves[user]} attempts')
            except Exception as e:
                self.logger.warning(f'[CHANGE ANS] [SUBMITTED BOTH ANS] User {user} Exception when DMing {e}')

        for user in submitted_old_only:
            try:
                await self.bot.get_user(user).send(
                    f'The answer has changed; the previous answers you submitted are now incorrect. ')
                self.logger.info(
                    f'[CHANGE ANS] [SUBMITTED OLD ONLY] User {user} solved after {new_solves[user]} attempts')
            except Exception as e:
                self.logger.warning(f'[CHANGE ANS] [SUBMITTED OLD ONLY] User {user} Exception when DMing {e}')

        # Sort out roles - give to those in new_ans and take from those in old_ans
        cursor.execute('SELECT server_id, solved_role_id from config where solved_role_id is not null')
        servers = self._filter_allowed_servers(cursor.fetchall())

        for user in submitted_new_only:
            await shared.assign_solved_role(servers, user, True, ctx)
        for user in submitted_old_only:
            await shared.assign_solved_role(servers, user, False, ctx)

        # Update DB rankings
        # Remove all solves
        cursor.execute('DELETE FROM solves where problem_id = ?', (problem.id,))
        self.bot.db.commit()

        # Add the new rankings
        cursor.executemany('INSERT INTO solves (user, problem_id, num_attempts, official) VALUES (?, ?, ?, ?)',
                           [
                               (user, problem.id, new_solves[user], True)
                               for user in new_solves])
        self.bot.db.commit()

        # Change the answer
        cursor.execute('UPDATE problems SET answer = ? WHERE id = ?', (new_answer, problem.id))
        self.bot.db.commit()

        # Update rankings
        self.bot.get_cog('Interface').update_rankings(problem.season)

    @commands.command(name='delete_thread')
    @commands.check(authorised)
    async def delete_thread(self, ctx):
        if not isinstance(ctx.channel, discord.Thread):
            await ctx.send('This command can only be used inside a thread.')
            return

        try:
            await ctx.channel.delete(reason=f'Deleted by authorised user {ctx.author.id}')
        except discord.Forbidden:
            await ctx.send('I do not have permission to delete this thread.')
        except discord.HTTPException as e:
            await ctx.send(f'Failed to delete thread: `{e}`')

    @app_commands.command(name='delete_thread', description='Delete the current thread.')
    async def delete_thread_slash(self, interaction: discord.Interaction):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        if not isinstance(interaction.channel, discord.Thread):
            await interaction.response.send_message('This command can only be used inside a thread.', ephemeral=True)
            return

        await interaction.response.send_message('Deleting this thread...', ephemeral=True)
        try:
            await interaction.channel.delete(reason=f'Deleted by authorised user {interaction.user.id}')
        except discord.Forbidden:
            await interaction.followup.send('I do not have permission to delete this thread.', ephemeral=True)
        except discord.HTTPException as e:
            await interaction.followup.send(f'Failed to delete thread: `{e}`', ephemeral=True)

    @commands.command(name='clean_dm')
    @commands.check(authorised)
    async def clean_dm(self, ctx, user: discord.User, minutes: int = None):
        default_window = self.bot.config.get('dm_cleanup_window_minutes', 1440)
        try:
            default_window = int(default_window)
        except (TypeError, ValueError):
            default_window = 1440

        lookback_minutes = default_window if minutes is None else minutes

        if lookback_minutes <= 0:
            await ctx.send('Time window must be a positive number of minutes.')
            return
        if lookback_minutes > 43200:
            await ctx.send('Time window is too large. Use at most 43200 minutes (30 days).')
            return

        summary = await self._clean_dm_impl(user, lookback_minutes)

        response = await ctx.send(f'{summary} React with 🗑️ to delete this message.')
        delete_emoji = '🗑️'
        try:
            await response.add_reaction(delete_emoji)
        except discord.HTTPException:
            return

        def reaction_check(reaction: discord.Reaction, reactor: discord.User):
            return (
                reaction.message.id == response.id
                and str(reaction.emoji) == delete_emoji
                and reactor.id == ctx.author.id
            )

        try:
            await self.bot.wait_for('reaction_add', timeout=120, check=reaction_check)
            await response.delete()
        except asyncio.TimeoutError:
            return
        except discord.HTTPException:
            return

    @app_commands.command(name='clean_dm', description='Delete bot DM messages to a user within a time window.')
    @app_commands.describe(minutes='Lookback window in minutes (defaults to config value)')
    async def clean_dm_slash(self, interaction: discord.Interaction, user: discord.User, minutes: int = None):
        if not interaction.user or not self._is_authorised_user(interaction.user.id):
            await interaction.response.send_message('You are not authorised to use this command.', ephemeral=True)
            return

        default_window = self.bot.config.get('dm_cleanup_window_minutes', 1440)
        try:
            default_window = int(default_window)
        except (TypeError, ValueError):
            default_window = 1440

        lookback_minutes = default_window if minutes is None else minutes
        if lookback_minutes <= 0:
            await interaction.response.send_message('Time window must be a positive number of minutes.', ephemeral=True)
            return
        if lookback_minutes > 43200:
            await interaction.response.send_message(
                'Time window is too large. Use at most 43200 minutes (30 days).',
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        summary = await self._clean_dm_impl(user, lookback_minutes)
        await interaction.followup.send(summary, ephemeral=True)


async def setup(bot: openpotd.OpenPOTD):
    await bot.add_cog(Management(bot))
