# Build release in elixir container
FROM elixir:1.9.2-alpine as builder

WORKDIR /app

ARG MIX_ENV=prod

ENV MIX_ENV $MIX_ENV

RUN apk add --no-cache git && \
    apk add --update --no-cache build-base

COPY . /app

RUN mix local.hex --force \
    && mix local.rebar --force \
    && mix do deps.get, \
      compile, \
      compile.protocols

RUN MIX_ENV=${MIX_ENV} mix release

# Build deployment container
FROM elixir:1.9.2-alpine

ARG MIX_ENV=prod
ARG PORT=4000

ENV MIX_ENV=$MIX_ENV \
    PORT=$PORT

EXPOSE 4000

WORKDIR /app

COPY --from=builder /app/_build/${MIX_ENV}/rel/orwell .

CMD ["/app/bin/orwell", "start"]
