# Contributing to NSQIP Tools

## Development Setup

1. Clone the repository
2. Install dependencies with `uv sync --all-extras`
3. Run tests with `uv run pytest`

## Code Style

- Use `ruff` for formatting and linting
- Follow Google-style docstrings
- Add type hints to all functions

## Security

- Never commit data files
- Never include PHI or patient information
- Check .gitignore before committing

## Pull Request Process

1. Create a feature branch
2. Make your changes with tests
3. Run `uv run ruff check --fix .`
4. Submit PR with clear description
