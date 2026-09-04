# Connector Manager

This is a helper tool for fetching podcast data from various sources using
our connectors.

## Installation

Install the dependencies:

```
make install
```

## Usage

Add your encryption key to your `.env` file:

```bash
OPENPODCAST_ENCRYPTION_KEY="my-secret-passphrase"
```

After that, you can run the tool with:

```bash
make run
```

To list the available command-line flags without loading configuration or
connecting to the database:

```bash
python -m manager --help
```
