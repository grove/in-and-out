"""Interactive REPL mode for in-and-out CLI."""
from __future__ import annotations

import os
import sys
from typing import Any

try:
    import readline
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

console = Console()


class InteractiveSession:
    """Interactive REPL session for in-and-out CLI."""
    
    def __init__(self, database_url: str, config_path: str | None = None):
        self.database_url = database_url
        self.config_path = config_path
        self.history_file = os.path.expanduser("~/.inandout_history")
        self._setup_readline()
    
    def _setup_readline(self):
        """Setup readline with history and tab completion."""
        if not HAS_READLINE:
            return
        
        # Load history
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            pass
        
        # Set history size
        readline.set_history_length(1000)
        
        # Setup tab completion
        readline.parse_and_bind("tab: complete")
        readline.set_completer(self._completer)
    
    def _completer(self, text: str, state: int) -> str | None:
        """Tab completion for commands."""
        commands = [
            "status",
            "pause",
            "resume",
            "force-sync",
            "reset-watermark",
            "reset-circuit-breaker",
            "gdpr-purge",
            "replay-dead-letter",
            "list-connectors",
            "list-datatypes",
            "show-sync-runs",
            "show-dead-letter",
            "drain",
            "reload-config",
            "help",
            "exit",
            "quit",
        ]
        
        matches = [cmd for cmd in commands if cmd.startswith(text)]
        if state < len(matches):
            return matches[state]
        return None
    
    def _save_history(self):
        """Save command history to file."""
        if HAS_READLINE:
            try:
                readline.write_history_file(self.history_file)
            except Exception:
                pass
    
    def _execute_command(self, command_line: str) -> bool:
        """Execute a command. Returns False to exit."""
        parts = command_line.strip().split()
        if not parts:
            return True
        
        cmd = parts[0].lower()
        args = parts[1:]
        
        if cmd in ("exit", "quit"):
            return False
        
        if cmd == "help":
            self._show_help()
            return True
        
        if cmd == "status":
            self._show_status(args)
            return True
        
        if cmd == "pause":
            self._pause_connector(args)
            return True
        
        if cmd == "resume":
            self._resume_connector(args)
            return True
        
        if cmd == "force-sync":
            self._force_sync(args)
            return True
        
        if cmd == "reset-circuit-breaker":
            self._reset_circuit_breaker(args)
            return True
        
        if cmd == "list-connectors":
            self._list_connectors()
            return True
        
        if cmd == "show-sync-runs":
            self._show_sync_runs(args)
            return True
        
        console.print(f"[red]Unknown command: {cmd}[/red]")
        console.print("Type 'help' for available commands.")
        return True
    
    def _show_help(self):
        """Display help text."""
        table = Table(title="Available Commands", show_header=True)
        table.add_column("Command", style="cyan")
        table.add_column("Description")
        
        table.add_row("status [connector] [datatype]", "Show connector/datatype status")
        table.add_row("pause <connector> [datatype]", "Pause ingestion for connector/datatype")
        table.add_row("resume <connector> [datatype]", "Resume paused connector/datatype")
        table.add_row("force-sync <connector> <datatype>", "Force full sync (reset watermark)")
        table.add_row("reset-circuit-breaker <connector>", "Reset circuit breaker state")
        table.add_row("list-connectors", "List all configured connectors")
        table.add_row("show-sync-runs [connector]", "Show recent sync runs")
        table.add_row("help", "Show this help message")
        table.add_row("exit / quit", "Exit interactive mode")
        
        console.print(table)
    
    def _show_status(self, args: list[str]):
        """Show status of connectors."""
        # This would query the control table and connector health
        console.print("[yellow]Status query not yet implemented in interactive mode[/yellow]")
        console.print("Use: inandout control status <connector> <datatype>")
    
    def _pause_connector(self, args: list[str]):
        """Pause a connector."""
        if not args:
            console.print("[red]Usage: pause <connector> [datatype][/red]")
            return
        
        console.print(f"[yellow]Pause command: {' '.join(args)}[/yellow]")
        console.print("Use: inandout control pause <connector> [datatype]")
    
    def _resume_connector(self, args: list[str]):
        """Resume a connector."""
        if not args:
            console.print("[red]Usage: resume <connector> [datatype][/red]")
            return
        
        console.print(f"[yellow]Resume command: {' '.join(args)}[/yellow]")
        console.print("Use: inandout control resume <connector> [datatype]")
    
    def _force_sync(self, args: list[str]):
        """Force full sync."""
        if len(args) < 2:
            console.print("[red]Usage: force-sync <connector> <datatype>[/red]")
            return
        
        console.print(f"[yellow]Force sync: {args[0]} / {args[1]}[/yellow]")
        console.print("Use: inandout control force-full-sync <connector> <datatype>")
    
    def _reset_circuit_breaker(self, args: list[str]):
        """Reset circuit breaker."""
        if not args:
            console.print("[red]Usage: reset-circuit-breaker <connector>[/red]")
            return
        
        console.print(f"[yellow]Reset circuit breaker: {args[0]}[/yellow]")
        console.print("Use: inandout control reset-circuit-breaker <connector>")
    
    def _list_connectors(self):
        """List configured connectors."""
        console.print("[yellow]List connectors not yet implemented[/yellow]")
        console.print("Use: inandout connector list")
    
    def _show_sync_runs(self, args: list[str]):
        """Show recent sync runs."""
        console.print("[yellow]Show sync runs not yet implemented[/yellow]")
        console.print("Use SQL query against inout_ops_sync_run table")
    
    def run(self):
        """Run the interactive REPL."""
        console.print("[bold cyan]In-and-Out Interactive Mode[/bold cyan]")
        console.print("Type 'help' for available commands, 'exit' to quit.\n")
        
        try:
            while True:
                try:
                    command_line = Prompt.ask("[bold green]inandout>[/bold green]")
                    if not self._execute_command(command_line):
                        break
                except KeyboardInterrupt:
                    console.print("\n[yellow]Use 'exit' or 'quit' to exit[/yellow]")
                    continue
                except EOFError:
                    break
        finally:
            self._save_history()
            console.print("[cyan]Goodbye![/cyan]")


def start_interactive(database_url: str, config_path: str | None = None):
    """Start an interactive REPL session."""
    session = InteractiveSession(database_url, config_path)
    session.run()
