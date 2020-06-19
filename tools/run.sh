#!/usr/bin/env bash
BASE_DIR=$(dirname "${BASH_SOURCE[0]}")
cmd=$1
shift

function generator {

    GPARAMS=""
    TPARAMS=""
    TT=""
    while [[ $# -gt 0 ]]
    do
      key="$1"

      case $key in
          -s)
          TPARAMS="$TPARAMS --sigma $2"
          shift 
          shift 
          ;; 
          -seed)
          GPARAMS="$GPARAMS -seed $2"
          TPARAMS="$TPARAMS --seed $2"
          shift 
          shift 
          ;; 
          -oo)
          TPARAMS="$TPARAMS -v 3"
          TT="y"
          shift
          ;;
          -et)
          TPARAMS="$TPARAMS -v 4"
          TT="y"
          shift
          ;;
          -md)
          TPARAMS="$TPARAMS --max_ooo $2"
          shift 
          shift 
          ;;
          -wp)
          TPARAMS="$TPARAMS --watermark_period $2"
          shift 
          shift 
          ;; 
          *) 
          GPARAMS="$GPARAMS $1"
          shift
          ;;
      esac
    done

  if [ -z "$TPARAMS" ]; then
    $BASE_DIR/generator.sh $GPARAMS
  else 
    if [ -z "$TT" ]; then
      $BASE_DIR/generator.sh $GPARAMS | $BASE_DIR/trace-transformer.sh -v 3 -n 1 -s false $TPARAMS
    else
      $BASE_DIR/generator.sh $GPARAMS | $BASE_DIR/trace-transformer.sh -n 1 -s false $TPARAMS
    fi
  fi

}

function oracle {

    PARAMS=""
    FORMULA=""
    while [[ $# -gt 0 ]]
    do
      key="$1"

      case $key in
          -S|-T|-L)
          FORMULA="$1"
          shift 
          ;;
          -w)
          INTERVAL="$2"
          shift 
          shift 
          ;;
          -P)
          FORMULA="$1 $2"
          shift # past argument
          shift # past value
          ;; 
          *) 
          PARAMS="$PARAMS $1"
          shift
          ;;
      esac
    done

  if [ -z "$FORMULA" ]; then
    verimon $PARAMS
  else 
    $BASE_DIR/generator.sh $FORMULA $INTERVAL -osig ../tmp.sig -oformula ../tmp.mfotl
    verimon -sig ../tmp.sig -formula ../tmp.mfotl $PARAMS
    rm ../tmp.sig
    rm ../tmp.mfotl
  fi
}

case "$cmd" in
  generator|Generator ) generator "$@" ;;
  replayer|Replayer ) $BASE_DIR/replayer.sh "$@";;
  oracle|Oracle ) oracle "$@" ;;
  * ) echo "Invalid command. Try 'generator', 'replayer' or 'oracle'";;
esac
