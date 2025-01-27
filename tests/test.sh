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

VALKEY_VERSION=${VALKEY_VERSION:-unstable}
PROGNAME="${BASH_SOURCE[0]}"
CWD="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $CWD/.. && pwd)
MODULE="$ROOT/target/debug/libvalkey_metrics${MODULE_EXT}"
VALKEY_SERVER="$CWD/.build/binaries/$VALKEY_VERSION/valkey-server"
PORT=${PORT:-6379}
VERBOSE=${VERBOSE:-0}
DEBUG=${DEBUG:-0}
EXT=${EXT:-run}
QUICK=${QUICK:-1}

# echo "CWD=$CWD, ROOT=$ROOT, MODULE_PATH=$MODULE"

if [[ -z $MODULE || ! -f $MODULE ]]; then
  echo "Module for version ${VALKEY_VERSION} not found at ${MODULE}. Aborting."
  exit 1
fi

help() {
	cat <<-'END'
		Run integration tests.

		[ARGVARS...] tests.sh [--help|help]

		Argument variables:
		TEST=test             Run specific test (e.g. test.py:test_name)

		RLTEST=path|'view'    Take RLTest from repo path or from local view
		RLTEST_ARGS=...       Extra RLTest arguments

		GEN=0|1               General tests on standalone Valkey (default)
		AOF=0|1               AOF persistency tests on standalone Valkey
		SLAVES=0|1            Replication tests on standalone Valkey
		AOF_SLAVES=0|1        AOF together SLAVES persistency tests on standalone Valkey
		CLUSTER=0|1           General tests on cluster
		SHARDS=n              Number of shards (default: 3)

		QUICK=1               Perform only one test variant

		TESTFILE=file         Run tests listed in `file`
		FAILEDFILE=file       Write failed tests into `file`

		PORT=n                Valkey server port

		EXT=1|run             Test on existing env (1=running; run=start redis-server)
		EXT_HOST=addr         Address of existing env (default: 127.0.0.1)

		COV=1                 Run with coverage analysis
		BB=1                  Enable Python debugger (break using BB() in tests)
		GDB=1                 Enable interactive gdb debugging (in single-test mode)

		RLTEST=path|'view'    Take RLTest from repo path or from local view
		RLTEST_ARGS=args      Extra RLTest args

		PARALLEL=1            Runs tests in parallel
		SLOW=1                Do not test in parallel
		UNIX=1                Use unix sockets
		RANDPORTS=1           Use randomized ports

		CLEAR_LOGS=0          Do not remove logs prior to running tests
		NOFAIL=1              Do not fail on errors (always exit with 0)

		LIST=1                List all tests and exit
    DEBUG=1               Show debugging printouts from tests
		VERBOSE=1             Print commands and Valkey output
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

	if [[ $VERBOSE == 1 ]]; then
		RLTEST_ARGS+=" -v"
	fi
	if [[ $DEBUG == 1 ]]; then
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

setup_server() {
	VALKEY_SERVER=${VALKEY_SERVER:-redis-server}

	if ! is_command $VALKEY_SERVER; then
		echo "Cannot find $VALKEY_SERVER. Aborting."
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
			cat <<-EOF > $rltest_config
				--oss-redis-path=$VALKEY_SERVER
				--module $MODULE
				--module-args '$MODARGS'
				$RLTEST_ARGS
				$RLTEST_TEST_ARGS
				$RLTEST_PARALLEL_ARG
				$RLTEST_COV_ARGS

				EOF
	else # existing env
		if [[ $EXT == run ]]; then
			xvalkey_conf=$(mktemp "${TMPDIR:-/tmp}/xvalkey_conf.XXXXXXX")
			rm -f $xvalkey_conf
			cat <<-EOF > $xvalkey_conf
				loadmodule $MODULE $MODARGS
				EOF

			rltest_config=$(mktemp "${TMPDIR:-/tmp}/xvalkey_rltest.XXXXXXX")
			rm -f $rltest_config
			cat <<-EOF > $rltest_config
				--env existing-env
				$RLTEST_ARGS
				$RLTEST_TEST_ARGS

				EOF

			if [[ $VERBOSE == 1 ]]; then
				echo "External valkey-server configuration:"
				cat $xvalkey_conf
			fi

			$VALKEY_SERVER $xvalkey_conf &
			XSERVER_PID=$!
			echo "External valkey-server pid: " XSERVER_PID

		else # EXT=1
			rltest_config=$(mktemp "${TMPDIR:-/tmp}/xvalkey_rltest.XXXXXXX")
			[[ $KEEP != 1 ]] && rm -f $rltest_config
			cat <<-EOF > $rltest_config
				--env existing-env
				--existing-env-addr $EXT_HOST:$PORT
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

	if [[ -n $XSERVER_PID ]]; then
		echo "killing external valkey-server: $XSERVER_PID"
		kill -TERM $XSERVER_PID
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

EXT_HOST=${EXT_HOST:-127.0.0.1}
PID=$$

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

if [[ -n $PORT ]]; then
	RLTEST_ARGS+="--redis-port $PORT"
fi

[[ $UNIX == 1 ]] && RLTEST_ARGS+=" --unix"
[[ $RANDPORTS == 1 ]] && RLTEST_ARGS+=" --randomize-ports"

#----------------------------------------------------------------------------------------------

setup_rltest
setup_server

#----------------------------------------------------------------------------------------------

if [[ $QUICK != 1 ]]; then
	GEN=${GEN:-1}
	SLAVES=${SLAVES:-1}
	AOF=${AOF:-1}
	AOF_SLAVES=${AOF_SLAVES:-1}
	CLUSTER=${CLUSTER:-1}
else
	GEN=1
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
	CLUSTER=0
fi

#-------------------------------------------------------------------------------- Running tests

if [[ $CLEAR_LOGS != 0 ]]; then
	rm -rf $CWD/logs
fi

E=0
[[ $GEN == 1 ]]         && { (run_tests "general tests"); (( E |= $? )); } || true
[[ $SLAVES == 1 ]]      && { (RLTEST_ARGS="${RLTEST_ARGS} --use-slaves" run_tests "tests with slaves"); (( E |= $? )); } || true
[[ $AOF == 1 ]]         && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof" run_tests "tests with AOF"); (( E |= $? )); } || true
[[ $AOF_SLAVES == 1 ]]  && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof --use-slaves" run_tests "tests with AOF and slaves"); (( E |= $? )); } || true
if [[ $CLUSTER == 1 ]]; then
	RLTEST_ARGS="${RLTEST_ARGS} --cluster_node_timeout 60000"
	if [[ -z $TEST || $TEST != test_ts_password ]]; then
		{ (RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" \
			run_tests "cluster tests"); (( E |= $? )); } || true
	fi
	if [[ -z $TEST || $TEST == test_ts_password* ]]; then
		RLTEST_ARGS_1="$RLTEST_ARGS"
		RLTEST_TEST_ARGS_1=" --test test_ts_password"
		{ (RLTEST_ARGS="${RLTEST_ARGS_1} --env oss-cluster --shards-count $SHARDS --oss_password password" \
		   RLTEST_TEST_ARGS="$RLTEST_TEST_ARGS_1" \
		   run_tests "tests on cluster with password"); (( E |= $? )); } || true
	fi
fi

#-------------------------------------------------------------------------------------- Summary

if [[ $NO_SUMMARY == 1 ]]; then
	exit 0
fi

if [[ $NOFAIL == 1 ]]; then
	exit 0
fi

exit $E