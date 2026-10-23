#!/bin/sh
# test | crap | demo | shell, or any command.
set -e
case "${1:-test}" in
	test) exec npm test ;;
	crap) exec npm run crap ;;
	demo) exec npm run demo ;;
	shell) exec /bin/sh ;;
	*) exec "$@" ;;
esac
