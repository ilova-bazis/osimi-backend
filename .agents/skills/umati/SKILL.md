# Umati Skill Documentation

## Overview

**Umati** is a workspace-local CLI task management tool designed for coordination between humans and AI coding agents. It provides a simple, file-based task system that supports task creation, assignment, lifecycle management, and archival.

### Why Use Umati?

- **Workspace-local**: Tasks live in `.umati/` within your project
- **AI-friendly**: Simple JSON format, clear CLI interface
- **Agent coordination**: Exclusive claiming prevents duplicate work
- **Hierarchy support**: Break down work into subtasks
- **Event history**: Track what happened to each task

## Installation

Umati is a Go binary. Install it and ensure it's in your PATH.

## Quick Start

Initialize a workspace:
```bash
umati init
```

Create your first task:
```bash
umati create --title "Implement auth" --description "Add JWT authentication" --status ready --agent human
```

View tasks:
```bash
umati list all
```

## Core Concepts

### Workspace

Umati operates within a workspace directory containing a `.umati/` folder:

```
.umati/
  config.json      # Workspace configuration
  tasks/           # Active task files
  deleted/         # Archived deleted tasks
  events/          # Event log (events.jsonl)
  .lock            # Workspace lock (during mutations)
```

### Task Lifecycle

Tasks flow through these statuses:

```
draft → ready → claimed → in_progress → done
  ↓       ↓         ↓           ↓
pause  cancel    pause      cancel
```

**Status meanings:**
- `draft` - Task is being written, not yet actionable
- `paused` - Intentionally not being worked on
- `ready` - Available for claiming
- `claimed` - Reserved by an agent (no other agent can claim)
- `in_progress` - Actively being implemented
- `done` - Completed
- `cancelled` - Abandoned
- `deleted` - Archived (not visible in normal operations)

### Agents

Valid agent identifiers:
- `opencode` - OpenCode agent
- `codex` - Codex agent
- `claude` - Claude agent
- `human` - Human user

**Important**: Always identify yourself with `--agent <name>` when running mutating commands.

## Command Reference

### Workspace Commands

#### `umati init [directory]`

Initialize a new umati workspace.

Options:
- `--id-prefix <prefix>` - Task ID prefix (default: UM)

Examples:
```bash
umati init                           # Initialize in current directory
umati init ./my-project              # Initialize in specific directory
umati init --id-prefix PROJ          # Use PROJ-1, PROJ-2, etc.
```

### Read Commands

#### `umati list all [filters]`

List all active tasks hierarchically.

Filters:
- `--status <status>` - Filter by status
- `--priority <priority>` - Filter by priority
- `--agent <agent>` - Filter by assignee

Example:
```bash
umati list all
umati list all --status ready
umati list all --priority high --agent human
```

#### `umati list ready [filters]`

List ready tasks with their descendants (regardless of descendant status).

Same filters as `list all`.

#### `umati list mine --agent <agent>`

List tasks assigned to you (claimed or in-progress).

Example:
```bash
umati list mine --agent claude
```

#### `umati show <task-id>`

Show full details for a task.

Example:
```bash
umati show UM-12
```

#### `umati search <query>`

Search tasks by title or description (case-insensitive).

Example:
```bash
umati search auth
umati search "api key"
```

### Write Commands (Mutations)

**All write commands require `--agent <name>` and acquire a workspace lock.**

#### `umati create [options]`

Create a new task.

Required:
- `--title <title>` - Task title
- `--agent <agent>` - Your agent identifier

Optional:
- `--description <text>` - Task description (required unless status=draft)
- `--priority <level>` - low|medium|high|urgent (default: medium)
- `--status <status>` - draft|paused|ready (default: draft)
- `--parent <task-id>` - Parent task for subtasks
- `-i, --interactive` - Interactive mode (prompts for fields)

Example:
```bash
umati create --title "Fix bug" --description "Fix the login bug" --priority high --status ready --agent human
umati create --title "Subtask" --description "Part of the work" --parent UM-12 --agent human
umati create -i  # Interactive mode
```

#### `umati update <task-id> [options]`

Update task fields.

Required:
- `--agent <agent>` - Your agent identifier

Optional (at least one required):
- `--title <title>` - New title
- `--description <text>` - New description
- `--priority <level>` - New priority
- `--status <status>` - New status (must be valid transition)
- `--parent <task-id>` - New parent (use 'none' for top-level)

Example:
```bash
umati update UM-12 --priority urgent --agent human
umati update UM-12 --status ready --agent human
umati update UM-12 --parent UM-5 --agent human
umati update UM-12 --parent none --agent human
```

#### `umati claim <task-id> --agent <agent>`

Claim a ready task exclusively.

Requirements:
- Task must be `ready`
- Task must have no assignee

Example:
```bash
umati claim UM-12 --agent claude
```

#### `umati start <task-id> --agent <agent>`

Start work on a claimed task.

Requirements:
- Task must be `claimed`
- You must be the claiming agent

Example:
```bash
umati start UM-12 --agent claude
```

#### `umati pause <task-id> --agent <agent>`

Pause a claimed or in-progress task.

Requirements:
- Task must be `claimed` or `in_progress`
- For in_progress, you must be the assignee

Effect:
- Status becomes `paused`
- Assignee is cleared

Example:
```bash
umati pause UM-12 --agent claude
```

#### `umati release <task-id> --agent <agent>`

Release a claimed task back to ready.

Requirements:
- Task must be `claimed` or `in_progress`
- You must be the assignee

Effect:
- Status becomes `ready`
- Assignee is cleared

Example:
```bash
umati release UM-12 --agent claude
```

#### `umati complete <task-id> --agent <agent>`

Mark an in-progress task as done.

Requirements:
- Task must be `in_progress`
- You must be the assignee
- All descendants must be `done` or `cancelled`

Effect:
- Status becomes `done`
- Assignee is cleared

Example:
```bash
umati complete UM-12 --agent claude
```

#### `umati delete <task-id> --agent <agent>`

Archive a task and its descendants.

Requirements:
- Task and all descendants must NOT be `claimed` or `in_progress`

Effect:
- Task and descendants are moved to `.umati/deleted/`
- Status becomes `deleted`
- Deleted tasks don't appear in normal commands

Example:
```bash
umati delete UM-12 --agent human
```

## Agent Workflow

### Starting Work

1. **Find available work:**
   ```bash
   umati list ready
   ```

2. **Inspect a task:**
   ```bash
   umati show UM-12
   ```

3. **Claim the task:**
   ```bash
   umati claim UM-12 --agent claude
   ```

4. **Start working:**
   ```bash
   umati start UM-12 --agent claude
   ```

5. **Do the implementation work** (outside umati)

### Finishing Work

1. **Complete the task:**
   ```bash
   umati complete UM-12 --agent claude
   ```

### If Blocked

1. **Pause the task:**
   ```bash
   umati pause UM-12 --agent claude
   ```

2. **Create a subtask or new task for the blocker:**
   ```bash
   umati create --title "Investigate blocker" --description "Need to understand X before continuing" --status ready --parent UM-12 --agent claude
   ```

### If You Can't Complete

1. **Release the task:**
   ```bash
   umati release UM-12 --agent claude
   ```

## Best Practices

### Do

- **Always claim before starting** - Prevents duplicate work
- **Always use `--agent`** - Required for mutations, helps tracking
- **Create subtasks for complex work** - Break down large tasks
- **Write clear descriptions** - Help others (and future you) understand
- **Update status promptly** - Keep the board accurate
- **Search before creating** - Avoid duplicate tasks
- **Use interactive mode** - `umati create -i` for guided task creation

### Don't

- **Don't work without claiming** - Someone else might pick it up
- **Don't complete parent tasks with unfinished children** - System will block this
- **Don't delete tasks with active work** - Claimed/in-progress tasks block deletion
- **Don't panic if locked** - Wait and retry, or check what's happening

## Common Workflows

### Human Creates Work for AI

```bash
# Create parent task
umati create --title "Implement API" --description "Build the REST API" --status ready --agent human

# Create subtasks
umati create --title "Setup routes" --description "Define API routes" --status ready --parent UM-1 --agent human
umati create --title "Add validation" --description "Input validation" --status draft --parent UM-1 --agent human
```

### AI Takes and Completes Work

```bash
# Find work
umati list ready

# Claim and start
umati claim UM-2 --agent claude
umati start UM-2 --agent claude

# [Do implementation work]

# Complete
umati complete UM-2 --agent claude
```

### Updating Task Priority

```bash
umati update UM-12 --priority urgent --agent human
```

### Moving a Subtask

```bash
# Make top-level
umati update UM-5 --parent none --agent human

# Move to different parent
umati update UM-5 --parent UM-10 --agent human
```

## Troubleshooting

### "workspace is locked by another operation"

Another agent is currently mutating the workspace. Wait a moment and retry.

### "cannot transition from X to Y"

You're trying an invalid state transition. Check the lifecycle diagram.

### "task is assigned to Z, not you"

You tried to modify a task claimed by another agent. Only the claiming agent can start/pause/release/complete.

### "cannot complete: unfinished descendants"

Complete all subtasks before completing a parent task.

### "cannot delete: active descendants"

A task in the subtree is claimed or in-progress. Release or complete those first.

## Tips for AI Agents

1. **Always inspect before claiming** - Read the full task with `show`
2. **Check for subtasks** - Parent tasks may have work items defined
3. **Use search** - `umati search auth` to find related tasks
4. **Update your tasks** - Use `umati list mine --agent <you>` to see your work
5. **Create clear titles** - Other agents and humans will read them
6. **Respect the hierarchy** - Complete leaf tasks before parents
