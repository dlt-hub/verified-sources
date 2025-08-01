The `/ai` directory includes rules for popular LLM-enabled Agents and IDEs (Amp, Claude, Cline, Codex, Cody, Continue, Copilot, Cursor, Windsurf).
Add them to your project using the `dlt ai setup $IDE` command ([reference](https://dlthub.com/docs/reference/command-line-interface#dlt-ai-setup)).

The way it works is very simple:

1. Each folder here corresponds to a particular IDE or AI agent.
2. The `setup` subcommand looks for a folder named `$IDE` and copies all files from it to current `dlt` project.
3. The `.message` file is displayed, not copied.

If you want to share useful rules or make changes to the existing ones, follow the instructions in [CONTRIBUTING.md](../CONTRIBUTING.md#walkthrough-modify-or-add-rules-files-for-llm-enabled-ides).