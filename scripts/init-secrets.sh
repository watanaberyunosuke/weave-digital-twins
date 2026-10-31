#!/usr/bin/env bash
# Fills the console's secrets in .env. Run it after `cp .env.example .env`
# and before the first `docker compose up`, on the host that runs the stack.
#
#   ./scripts/init-secrets.sh           # tokens, session secret, admin password
#   ./scripts/init-secrets.sh --totp    # also an authenticator secret
#
# Only blank values are filled, so a re-run never changes a token the
# consoles already hold or logs anyone out. Nothing secret is printed: the
# tokens are read only by the two consoles, the admin password is kept only
# as its hash, and the authenticator enrolment goes to a file only the owner
# can read, because it has to be scanned once.
#
# On an unattended deploy, pass the hash in the environment instead:
#   CONSOLE_ADMIN_PASSWORD_HASH=scrypt:... ./scripts/init-secrets.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ENV_FILE=${ENV_FILE:-.env}
ENROL_FILE=.weave-totp-enrol.txt
want_totp=0
for arg in "$@"; do
  case $arg in
    --totp) want_totp=1 ;;
    *) echo "usage: $0 [--totp]" >&2; exit 2 ;;
  esac
done

if [[ ! -f $ENV_FILE ]]; then
  echo "! no $ENV_FILE — cp .env.example .env and set your region first"
  exit 1
fi
chmod 600 "$ENV_FILE"

get() { sed -n "s/^$1=//p" "$ENV_FILE" | tail -n 1; }

# Replaces KEY=... in place, or appends it. Values here are hex, base32 or
# base64url, so awk's escape handling in -v never alters them. mktemp
# creates the file 0600, so the secrets are never briefly world-readable.
set_var() {
  local tmp
  tmp=$(mktemp "$ENV_FILE.XXXXXX")
  awk -v k="$1" -v v="$2" '
    index($0, k "=") == 1 { if (!done) print k "=" v; done = 1; next }
    { print }
    END { if (!done) print k "=" v }' "$ENV_FILE" > "$tmp"
  mv "$tmp" "$ENV_FILE"
}

random_token() { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

need_python() {
  command -v python3 > /dev/null || { echo "! python3 is needed for $1"; exit 1; }
}

filled=()
for key in CONSOLE_RELAY_TOKEN CONSOLE_RELAY_ADMIN_TOKEN CONSOLE_SESSION_SECRET; do
  if [[ -z $(get "$key") ]]; then
    set_var "$key" "$(random_token)"
    filled+=("$key")
  fi
done

if [[ -z $(get CONSOLE_ADMIN_PASSWORD_HASH) ]]; then
  if [[ -n ${CONSOLE_ADMIN_PASSWORD_HASH:-} ]]; then
    if [[ $CONSOLE_ADMIN_PASSWORD_HASH != scrypt:* ]]; then
      echo "! CONSOLE_ADMIN_PASSWORD_HASH is not a hash from console/access.py admin-hash"
      exit 1
    fi
    set_var CONSOLE_ADMIN_PASSWORD_HASH "$CONSOLE_ADMIN_PASSWORD_HASH"
    filled+=(CONSOLE_ADMIN_PASSWORD_HASH)
  elif [[ -t 0 ]]; then
    need_python "the admin password hash"
    echo "Choose the admin password for the public console (14+ characters)."
    line=$(python3 console/access.py admin-hash)
    set_var CONSOLE_ADMIN_PASSWORD_HASH "${line#CONSOLE_ADMIN_PASSWORD_HASH=}"
    filled+=(CONSOLE_ADMIN_PASSWORD_HASH)
  else
    # Not fatal: the public console works without it, just with no admin login.
    echo "  admin login stays off: no terminal to ask for the password and no"
    echo "  CONSOLE_ADMIN_PASSWORD_HASH in the environment"
  fi
fi

if (( want_totp )) && [[ -z $(get CONSOLE_ADMIN_TOTP_SECRET) ]]; then
  need_python "the authenticator secret"
  secret=$(python3 -c 'import base64, secrets; print(base64.b32encode(secrets.token_bytes(20)).decode())')
  set_var CONSOLE_ADMIN_TOTP_SECRET "$secret"
  (umask 077; printf 'otpauth://totp/Weave:admin?secret=%s&issuer=Weave\n' "$secret" > "$ENROL_FILE")
  filled+=(CONSOLE_ADMIN_TOTP_SECRET)
  echo "  authenticator: add the link in $ENROL_FILE to your app, then delete the file"
fi

if (( ${#filled[@]} )); then
  echo "Filled in $ENV_FILE: ${filled[*]}"
  echo "Restart the consoles if they are running: docker compose up -d console console-public"
else
  echo "Nothing to fill; $ENV_FILE already has every console secret."
fi
