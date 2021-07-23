# opni-nats-wrapper

This repo contains shared code used to build the pypi package https://pypi.org/project/opni-nats/0.0.0.1/ which is used in several opni services

## Testing
1. have your nats server running in another terminal. For Mac user, you can do:
```
brew install nats-server
nats-server
```

2. test the script: (assume your nats server is running on nats://0.0.0.0:4222)
```
export NATS_SERVER_URL="nats://0.0.0.0:4222"
cd src/opni_nats
python nats_wrapper.py
```

## Contributing
We use `pre-commit` for formatting auto-linting and checking import. Please refer to [installation](https://pre-commit.com/#installation) to install the pre-commit or run `pip install pre-commit`. Then you can activate it for this repo. Once it's activated, it will lint and format the code when you make a git commit. It makes changes in place. If the code is modified during the reformatting, it needs to be staged manually.

```
# Install
pip install pre-commit

# Install the git commit hook to invoke automatically every time you do "git commit"
pre-commit install

# (Optional)Manually run against all files
pre-commit run --all-files
```
