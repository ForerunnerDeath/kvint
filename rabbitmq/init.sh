#!/bin/sh
set -eu

user="${RABBITMQ_DEFAULT_USER:-}"
password="${RABBITMQ_DEFAULT_PASS:-}"
case "$user" in
    ''|*[!a-zA-Z0-9_-]*)
        echo >&2 'RABBITMQ_DEFAULT_USER must contain only letters, digits, _ or -'
        exit 1
        ;;
esac
if [ -z "$password" ]; then
    echo >&2 'RABBITMQ_DEFAULT_PASS must not be empty'
    exit 1
fi

# Boot-time definitions suppress RabbitMQ's automatic default-user creation.
# Generate only a salted password hash; the plaintext stays in the environment.
hash="$(rabbitmqctl hash_password "$password" | tail -n 1)"
case "$hash" in
    ''|*[!a-zA-Z0-9+/=]*)
        echo >&2 'Failed to generate RabbitMQ password hash'
        exit 1
        ;;
esac

sed -e "s|__RABBITMQ_USER__|$user|g" \
    -e "s|__PASSWORD_HASH__|$hash|g" \
    /etc/rabbitmq/definitions.template.json > /var/lib/rabbitmq/definitions.json
chmod 600 /var/lib/rabbitmq/definitions.json
exec docker-entrypoint.sh rabbitmq-server
