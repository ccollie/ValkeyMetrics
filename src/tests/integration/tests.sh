#!/bin/bash

# [[ $VERBOSE == 1 ]] && set -x
os_type=$(uname)
MODULE_EXT=".so"
if [[ "$os_type" == "Darwin" ]]; then
  MODULE_EXT=".dylib"
elif [[ "$os_type" == "Linux" ]]; then
  MODULE_EXT=".so"
elif [[ "$os_type" == "Windows" ]]; then
  MODULE_EXT=".dll"
else
  echo "Unsupported OS type: $os_type"
  exit 1
fi

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/../../.. && pwd)
MODULE="$ROOT/target/debug/libvalkey_metrics${MODULE_EXT}"

echo "HERE=$HERE, ROOT=$ROOT, MODULE_PATH=$MODULE"
export PYTHONUNBUFFERED=1

# cd $HERE

#----------------------------------------------------------------------------------------------

help() {
	cat <<-'END'
		Run integration tests.

		[ARGVARS...] tests.sh [--help|help] [<module-so-path>]

		Argument variables:
		MODULE=path           Module .so path

		TEST=test             Run specific test (e.g. test.py:test_name)

		RLTEST=path|'view'    Take RLTest from repo path or from local view
		RLTEST_ARGS=...       Extra RLTest arguments

		GEN=0|1               General tests on standalone Redis (default)
		AOF=0|1               AOF persistency tests on standalone Redis
		SLAVES=0|1            Replication tests on standalone Redis
		AOF_SLAVES=0|1        AOF together SLAVES persistency tests on standalone Redis
		OSS_CLUSTER=0|1       General tests on Redis OSS Cluster
		SHARDS=n              Number of shards (default: 3)

		QUICK=1               Perform only one test variant

		TEST=name             Run specific test (e.g. test.py:test_name)
		TESTFILE=file         Run tests listed in `file`
		FAILEDFILE=file       Write failed tests into `file`

		UNSTABLE=1            Do not skip unstable tests (default: 0)
		ONLY_STABLE=1         Skip unstable tests

		REDIS_SERVER=path     Location of redis-server
		REDIS_PORT=n          Redis server port

		EXT=1|run             Test on existing env (1=running; run=start redis-server)
		EXT_HOST=addr         Address of existing env (default: 127.0.0.1)
		EXT_PORT=n            Port of existing env

		DOCKER_HOST=addr      Address of Docker server (default: localhost)

		COV=1                 Run with coverage analysis
		BB=1                  Enable Python debugger (break using BB() in tests)
		GDB=1                 Enable interactive gdb debugging (in single-test mode)

		RLTEST=path|'view'    Take RLTest from repo path or from local view
		RLTEST_DEBUG=1        Show debugging printouts from tests
		RLTEST_ARGS=args      Extra RLTest args

		PARALLEL=1            Runs tests in parallel
		SLOW=1                Do not test in parallel
		UNIX=1                Use unix sockets
		RANDPORTS=1           Use randomized ports

		PLATFORM_MODE=1       Implies NOFAIL & COLLECT_LOGS into STATFILE
		COLLECT_LOGS=1        Collect logs into .tar file
		CLEAR_LOGS=0          Do not remove logs prior to running tests
		NOFAIL=1              Do not fail on errors (always exit with 0)
		STATFILE=file         Write test status (0|1) into `file`

		LIST=1                List all tests and exit
		VERBOSE=1             Print commands and Redis output
		LOG=1                 Send results to log (even on single-test mode)
		KEEP=1                Do not remove intermediate files
		NOP=1                 Dry run
		HELP=1                Show help

	END
}

#----------------------------------------------------------------------------------------------

is_command() {
    local cmd="$1"

    if [ -x "$cmd" ]; then
        if file "$cmd" | grep -qE "executable|Mach-O|ELF"; then
            echo "$cmd is an executable file"
            return 0
        else
            echo "$cmd has execute permissions but is not an executable file"
            return 1
        fi
    elif command -v "$cmd" >/dev/null 2>&1; then
        echo "$cmd is in PATH and executable"
        return 0
    else
        echo "$cmd is not executable or doesn't exist"
        return 1
    fi
}

traps() {
	local func="$1"
	shift
	local sig
	for sig in "$@"; do
		trap "$func $sig" "$sig"
	done
}

linux_stop() {
	local pgid=$(cat /proc/$PID/status | grep pgid | awk '{print $2}')
	kill -9 -- -$pgid
}

macos_stop() {
	local pgid=$(ps -o pid,pgid -p $PID | awk "/$PID/"'{ print $2 }' | tail -1)
	pkill -9 -g $pgid
}

stop() {
	trap - SIGINT
	if [[ "$os_type" == "Darwin" ]]; then
    macos_stop
  elif [[ "$os_type" == "Linux" ]]; then
    linux_stop
  fi
	exit 1
}

traps 'stop' SIGINT

#----------------------------------------------------------------------------------------------

setup_rltest() {
	if [[ $RLTEST == view ]]; then
		if [[ ! -d $ROOT/../RLTest ]]; then
			eprint "RLTest not found in view $ROOT"
			exit 1
		fi
		RLTEST=$(cd $ROOT/../RLTest; pwd)
	fi

	if [[ -n $RLTEST ]]; then
		if [[ ! -d $RLTEST ]]; then
			eprint "Invalid RLTest location: $RLTEST"
			exit 1
		fi

		# Specifically search for it in the specified location
		export PYTHONPATH="$PYTHONPATH:$RLTEST"
		if [[ $VERBOSE == 1 ]]; then
			echo "PYTHONPATH=$PYTHONPATH"
		fi
	fi

	if [[ $RLTEST_VERBOSE == 1 ]]; then
		RLTEST_ARGS+=" -v"
	fi
	if [[ $RLTEST_DEBUG == 1 ]]; then
		RLTEST_ARGS+=" --debug-print"
	fi
	if [[ -n $RLTEST_LOG && $RLTEST_LOG != 1 ]]; then
		RLTEST_ARGS+=" -s"
	fi
	if [[ $RLTEST_CONSOLE == 1 ]]; then
		RLTEST_ARGS+=" -i"
	fi
	RLTEST_ARGS+=" --enable-debug-command --enable-protected-configs"
}

#----------------------------------------------------------------------------------------------

setup_redis_server() {
	REDIS_SERVER=${REDIS_SERVER:-redis-server}

	if ! is_command $REDIS_SERVER; then
		echo "Cannot find $REDIS_SERVER. Aborting."
		exit 1
	fi
}

#----------------------------------------------------------------------------------------------

run_tests() {
	local title="$1"
	shift

	if [[ $EXT != 1 ]]; then
		rltest_config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
		rm -f $rltest_config
		if [[ $RLEC != 1 ]]; then
			cat <<-EOF > $rltest_config
				--oss-redis-path=$REDIS_SERVER
				--module $MODULE
				--module-args '$MODARGS'
				$RLTEST_ARGS
				$RLTEST_TEST_ARGS
				$RLTEST_PARALLEL_ARG
				$RLTEST_COV_ARGS

				EOF
		else
			cat <<-EOF > $rltest_config
				$RLTEST_ARGS

				EOF
		fi
	else # existing env
		if [[ $EXT == run ]]; then
			xredis_conf=$(mktemp "${TMPDIR:-/tmp}/xredis_conf.XXXXXXX")
			rm -f $xredis_conf
			cat <<-EOF > $xredis_conf
				loadmodule $MODULE $MODARGS
				EOF

			rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
			rm -f $rltest_config
			cat <<-EOF > $rltest_config
				--env existing-env
				$RLTEST_ARGS
				$RLTEST_TEST_ARGS

				EOF

			if [[ $VERBOSE == 1 ]]; then
				echo "External redis-server configuration:"
				cat $xredis_conf
			fi

			$REDIS_SERVER $xredis_conf &
			XREDIS_PID=$!
			echo "External redis-server pid: " $XREDIS_PID

		else # EXT=1
			rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
			[[ $KEEP != 1 ]] && rm -f $rltest_config
			cat <<-EOF > $rltest_config
				--env existing-env
				--existing-env-addr $EXT_HOST:$EXT_PORT
				$RLTEST_ARGS
				$RLTEST_TEST_ARGS

				EOF
		fi
	fi


	if [[ $VERBOSE == 1 || $NOP == 1 ]]; then
		echo "RLTest configuration:"
		cat $rltest_config
	fi

	local E=0
	if [[ $NOP != 1 ]]; then
		{ $OP python3 -m RLTest @$rltest_config; (( E |= $? )); } || true
	else
		$OP python3 -m RLTest @$rltest_config
	fi

	[[ $KEEP != 1 ]] && rm -f $rltest_config

	if [[ -n $XREDIS_PID ]]; then
		echo "killing external redis-server: $XREDIS_PID"
		kill -TERM $XREDIS_PID
	fi

	if [[ -n $GITHUB_ACTIONS ]]; then
		echo "::endgroup::"
	fi
	return $E
}

#------------------------------------------------------------------------------------ Arguments

if [[ $1 == --help || $1 == help || $HELP == 1 ]]; then
	help
	exit 0
fi

OP=""
[[ $NOP == 1 ]] && OP=echo

#--------------------------------------------------------------------------------- Environments

DOCKER_HOST=${DOCKER_HOST:-127.0.0.1}

EXT_HOST=${EXT_HOST:-127.0.0.1}
EXT_PORT=${EXT_PORT:-6379}

PID=$$

#---------------------------------------------------------------------------------- Tests scope
MODULE="${MODULE:-$1}"
if [[ -z $MODULE || ! -f $MODULE ]]; then
  echo "Module not found at ${MODULE}. Aborting."
  exit 1
fi

SHARDS=${SHARDS:-3}

#------------------------------------------------------------------------------------ Debugging
GDB=${GDB:-0}

if [[ $GDB == 1 ]]; then
	[[ $LOG != 1 ]] && RLTEST_LOG=0
	RLTEST_CONSOLE=1
fi

if [[ -n $TEST ]]; then
	[[ $LOG != 1 ]] && RLTEST_LOG=0
	# export BB=${BB:-1}
	export RUST_BACKTRACE=1
fi

#-------------------------------------------------------------------------------- Platform Mode

if [[ $PLATFORM_MODE == 1 ]]; then
	CLEAR_LOGS=0
	NOFAIL=1
fi
STATFILE=${STATFILE:-$ROOT/tests/integration/status}

#---------------------------------------------------------------------------------- Parallelism

[[ $SLOW == 1 ]] && PARALLEL=0

PARALLEL=${PARALLEL:-1}

# due to Python "Can't pickle local object" problem in RLTest
[[ "$os_type" == "Darwin" ]] && PARALLEL=0

[[ $EXT == 1 || $EXT == run || $BB == 1 || $GDB == 1 ]] && PARALLEL=0

if [[ -n $PARALLEL && $PARALLEL != 0 ]]; then
  parallel="$PARALLEL"
	RLTEST_PARALLEL_ARG="--parallelism $parallel"
fi

#------------------------------------------------------------------------------- Test selection

if [[ -n $TEST ]]; then
	RLTEST_TEST_ARGS+=$(echo -n " "; echo "$TEST" | awk 'BEGIN { RS=" "; ORS=" " } { print "--test " $1 }')
fi

if [[ -n $TESTFILE && -z $TEST ]]; then
	if ! is_abspath "$TESTFILE"; then
		TESTFILE="$ROOT/$TESTFILE"
	fi
	RLTEST_TEST_ARGS+=" -f $TESTFILE"
fi

if [[ -n $FAILEDFILE ]]; then
	if ! is_abspath "$FAILEDFILE"; then
		TESTFILE="$ROOT/$FAILEDFILE"
	fi
	RLTEST_TEST_ARGS+=" -F $FAILEDFILE"
	RLTEST_TEST_ARGS_1+=" -F $FAILEDFILE"
fi

if [[ $LIST == 1 ]]; then
	NO_SUMMARY=1
	RLTEST_ARGS+=" --collect-only"
fi

#---------------------------------------------------------------------------------------- Setup

if [[ $VERBOSE == 1 ]]; then
	RLTEST_VERBOSE=1
fi

RLTEST_LOG=${RLTEST_LOG:-$LOG}

if [[ $COV == 1 ]]; then
	setup_coverage
fi

RLTEST_ARGS+=" $@"

if [[ -n $REDIS_PORT ]]; then
	RLTEST_ARGS+="--redis-port $REDIS_PORT"
fi

[[ $UNIX == 1 ]] && RLTEST_ARGS+=" --unix"
[[ $RANDPORTS == 1 ]] && RLTEST_ARGS+=" --randomize-ports"

#----------------------------------------------------------------------------------------------

setup_rltest
setup_redis_server

#----------------------------------------------------------------------------------------------

if [[ $QUICK != 1 ]]; then
	GEN=${GEN:-1}
	SLAVES=${SLAVES:-1}
	AOF=${AOF:-1}
	AOF_SLAVES=${AOF_SLAVES:-1}
	OSS_CLUSTER=${OSS_CLUSTER:-1}
else
	GEN=1
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
	OSS_CLUSTER=0
fi

#-------------------------------------------------------------------------------- Running tests

if [[ $CLEAR_LOGS != 0 ]]; then
	rm -rf $HERE/logs
fi

E=0
[[ $GEN == 1 ]]         && { (run_tests "general tests"); (( E |= $? )); } || true
[[ $SLAVES == 1 ]]      && { (RLTEST_ARGS="${RLTEST_ARGS} --use-slaves" run_tests "tests with slaves"); (( E |= $? )); } || true
[[ $AOF == 1 ]]         && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof" run_tests "tests with AOF"); (( E |= $? )); } || true
[[ $AOF_SLAVES == 1 ]]  && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof --use-slaves" run_tests "tests with AOF and slaves"); (( E |= $? )); } || true
if [[ $OSS_CLUSTER == 1 ]]; then
	RLTEST_ARGS="${RLTEST_ARGS} --cluster_node_timeout 60000"
	if [[ -z $TEST || $TEST != test_ts_password ]]; then
		{ (RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" \
			run_tests "tests on OSS cluster"); (( E |= $? )); } || true
	fi
	if [[ -z $TEST || $TEST == test_ts_password* ]]; then
		RLTEST_ARGS_1="$RLTEST_ARGS"
		RLTEST_TEST_ARGS_1=" --test test_ts_password"
		{ (RLTEST_ARGS="${RLTEST_ARGS_1} --env oss-cluster --shards-count $SHARDS --oss_password password" \
		   RLTEST_TEST_ARGS="$RLTEST_TEST_ARGS_1" \
		   run_tests "tests on OSS cluster with password"); (( E |= $? )); } || true
	fi
fi

#-------------------------------------------------------------------------------------- Summary

if [[ $NO_SUMMARY == 1 ]]; then
	exit 0
fi

if [[ -n $STATFILE ]]; then
	mkdir -p "$(dirname "$STATFILE")"
	if [[ -f $STATFILE ]]; then
		(( E |= $(cat $STATFILE || echo 1) )) || true
	fi
	echo $E > $STATFILE
fi

if [[ $NOFAIL == 1 ]]; then
	exit 0
fi

exit $E