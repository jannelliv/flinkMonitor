{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
{-# OPTIONS_GHC -fno-cse #-}
module TestArgs where
import Prelude hiding (FilePath)
import System.Console.CmdArgs
import Shelly
import Data.Text as T
import Control.Lens
import Control.Lens.TH
default (T.Text)

data Config = Config { _eventrate :: Int
                     , _indexrate :: Int
                     , _loglength :: Int
                     , _sockhost :: Text
                     , _sockport :: Int
                     , _formula :: Text
                     , _sig :: Text
                     , _processors :: Int
                     , _kafkaparts :: Int
                     , _multisourcevariant :: Int
                     , _usereplayer :: Bool
                     , _usekafka :: Bool
                     , _replayeraccel :: Float
                     , _sigma :: Float
                     , _maxooo :: Int
                     , _watermarkperiod :: Int
                     , _novalidate :: Bool
                     , _insertcheckpoint :: Bool
                     , _flinkdir :: Text
                     , _noclear :: Bool
                     }
                deriving (Show, Data, Typeable)

data Ctxt = Ctxt { _workDir :: FilePath
                 , _jarPath :: FilePath
                 , _dataDir :: FilePath
                 , _sigFile :: FilePath
                 , _formulaFile :: FilePath
                 , _traceTransformer :: FilePath
                 , _traceGenerator :: FilePath
                 , _replayer :: FilePath
                 , _monpolyExe :: FilePath
                 , _flinkExe :: FilePath
                 , _shouldCollapse :: Bool
                 , _inputArgs :: Config
                 }

makeFieldsNoPrefix ''Config
makeFieldsNoPrefix ''Ctxt

baseconfig :: Mode (CmdArgs Config)
baseconfig = cmdArgsMode $ Config{ _eventrate = 1000 &= explicit &= name "eventrate" &= name "e" &= typ "INT" &= help "event rate of the generated log"
                   , _indexrate = 10 &= explicit &= name "indexrate" &= name "i" &= typ "INT" &= help "index rate of the generated log"
                   , _loglength = 60 &= explicit &= name "loglength" &= name "l" &= typ "INT" &= help "length of the generated log"
                   , _sockhost = "127.0.0.1" &= explicit &= name "socket_host" &= typ "STRING" &= help "the ip of the sockets"
                   , _sockport = 6060 &= explicit &= name "port" &= typ "INT" &= help "the port if sockets are used instead of kafka"
                   , _formula = "triangle-neg.mfotl" &= explicit &= name "formula" &= name "f" &= typ "FORMULA_NAME" &= help "the formula to monitor"
                   , _sig = "synth.sig" &= explicit &= name "signature" &= name "s" &= typ "SIGNATURE_NAME" &= help "signature name of the formula"
                   , _processors = 2 &= explicit &= name "procs" &= name "p" &= typ "INT" &= help "number of monitors"
                   , _kafkaparts = 4 &= explicit &= name "nparts" &= name "n" &= typ "INT" &= help "number of input partitions"
                   , _multisourcevariant = 1 &= explicit &= name "variant" &= name "m" &= typ "INT" &= help "multisource variant to use"
                   , _usereplayer = False &= explicit &= name "replayer" &= name "r" &= typ "BOOL" &= help "should use replayer"
                   , _usekafka = False &= explicit &= name "kafka" &= name "k" &= typ "BOOL" &= help "should use kafka"
                   , _replayeraccel = 1.0 &= explicit &= name "accel" &= name "a" &= typ "FLOAT" &= help "replayer acceleration"
                   , _novalidate = False &= explicit &= name "novalidate" &= help "don't generate and validate reference output"
                   , _insertcheckpoint = False &= explicit &= name "insertcheckpoint" &= help "test the snapshotting mode"
                   , _watermarkperiod = 2 &= explicit &= name "watermarkperiod" &= help "time interval for the watermarks (used if variant = 4)"
                   , _sigma = 2.0 &= explicit &= name "sigma" &= help "sigma for the truncated normal distribution (used if variant = 4)"
                   , _maxooo = 5 &= explicit &= name "maxooo" &= help "max out of orderness (used if variant = 4)"
                   , _flinkdir = "" &= argPos 0 &= typ "FLINK_DIR"
                   , _noclear = False &= explicit &= name "noclear" &= help "don't clear the temp files after the program finished"}
            &= help "Run synthetic tests for the parallel monitor"
            &= program "Tester"
    
parseArgs :: IO Config
parseArgs = cmdArgsRun baseconfig

makeContext :: IO Ctxt
makeContext = do
    args <- parseArgs
    currDir <- shelly pwd
    when (args^.multisourcevariant < 1 || args^.multisourcevariant > 4) $
        fail "multisource variant must be in 1 to 4"
    when (args^.usereplayer && args^.multisourcevariant == 3) $
        fail "multisouce variant 3 (watermark order without explicit emissiontime) \
                \is not compatible with the replayer"
    when ((not $ args^.usereplayer) && args^.multisourcevariant == 4) $
        fail "multisource variant 4 (watermark order with explicit emissiontime) \
                \is only compatible with the replayer"
    when ((not $ args^.usereplayer) && (args^.insertcheckpoint)) $
        fail "resuming from a checkpoint can only be used in combination with the replayer" 
    let workDir = currDir </> ".."
    let data_dir = workDir </> "evaluation"
    return Ctxt { _workDir = workDir
                , _jarPath = workDir </> "flink-monitor" </> "target" </> "flink-monitor-1.0-SNAPSHOT.jar"
                , _dataDir = data_dir
                , _sigFile = data_dir </> "synthetic" </> (args^.sig)
                , _formulaFile = data_dir </> "synthetic" </> (args^.formula)
                , _traceTransformer = workDir </> "trace-transformer.sh"
                , _traceGenerator = workDir </> "generator.sh"
                , _replayer = workDir </> "replayer.sh"
                , _monpolyExe = "monpoly"
                , _flinkExe = (args^.flinkdir) </> "bin" </> "flink"
                , _shouldCollapse = args^.multisourcevariant /= 1
                , _inputArgs = args
                }
