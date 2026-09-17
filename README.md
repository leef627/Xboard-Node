# xboard-node — Docker Compose

## Quick start

Use a Linux Docker host with a current Docker Compose plugin.
This branch contains deployment files; the Docker build downloads the application
source from the configured Git repository and compiles it inside a builder image.

```bash
git clone -b compose --depth 1 https://github.com/leef627/xboard-node.git
cd xboard-node
cp config/config.yml.example config/config.yml
# Edit config/config.yml: panel.url, panel.token, panel.node_id
docker compose up -d
```

By default, the builder runs `git clone` against
`https://github.com/leef627/xboard-node.git`, branch `dev`. The host only needs
Docker and these deployment files; it does not need a Go installation or a
checkout of the application source.

## Build source

Optionally copy `.env.example` to `.env` and change:

```dotenv
SOURCE_REPO=https://github.com/leef627/xboard-node.git
SOURCE_REF=dev
```

`SOURCE_REF` accepts a branch or tag supported by `git clone --branch`.
It does not accept a raw commit SHA.

The Dockerfile follows the multi-stage build used on `dev`:

1. A Go 1.26 Alpine builder installs Git, clones the source, downloads Go modules,
   and compiles the node with QUIC, uTLS, WireGuard, and Clash API support.
2. An Alpine runtime image receives only the compiled `xboard-node` executable,
   CA certificates, and timezone data.
3. The container starts the executable with the configuration mounted from the host.

Source download and compilation happen during **image building**. Restarting an
existing container starts its existing binary; rebuilding is how code is updated.

## Startup and source updates

`docker compose up -d` checks for the local image `xboard-node:local`:

- If the image exists, Compose starts it without building or downloading source.
- If the image is missing, Compose builds it using this Dockerfile, then starts it.

`pull_policy: never` skips pulling the application image from a registry; the
`build` definition supplies a missing local image. Existing images are reused
even if the Git branch or `.env` source settings have changed. Use
`docker compose up -d --build` when you want to update the application explicitly.

`build.no_cache: true` applies only when a build actually runs. That build clones
the source again, downloads dependencies, and compiles without reusing Docker
build layers or persistent Go cache mounts. This setting does not trigger a
build on its own.

The build requires access to the base-image registry, the source Git repository,
and Go dependencies. The executable version contains the Git description or SHA.

## Update / logs / stop

Run these commands from the same deployment checkout:

```bash
git pull --ff-only                  # update deployment files when needed
docker compose up -d --build        # clone current source, compile, and recreate
docker compose logs -f --tail=100
docker compose down
```

For separate build and startup steps:

```bash
docker compose build
docker compose up -d --no-build
```

## Layout

| Path | Purpose |
|------|---------|
| `compose.yml` | Local image build and service definition |
| `Dockerfile` | Clone and compile application source inside the builder |
| `config/config.yml.example` | Tracked node configuration template |
| `config/config.yml` | Local node configuration, ignored by Git |
| `.env.example` | Optional Git source settings |
| `.env` | Local source overrides, ignored by Git |

The host `config` directory is mounted at `/etc/xboard-node` and survives
container rebuilds and `docker compose down`. The Dockerfile does not copy host
configuration into the image. Copy the tracked `config/config.yml.example` to
`config/config.yml` on first deployment and fill in your panel settings. Everything
under `config/` except `config.yml.example` is ignored by Git; keep your local
configuration when updating deployment files.

## Host network

`network_mode: host` is set so the node uses the host network stack (same as `docker run --network host`). Free the ports your protocols need.

## Image

The built image is tagged `xboard-node:local`. Application images are built
locally; the Go and Alpine base images are obtained through Docker as needed.
