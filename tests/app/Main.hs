{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
module Main where
import Shelly
import Data.Text as T
import Lib
import TestArgs
import Prelude hiding (FilePath)
import Control.Lens
import Formatting
import Control.Monad
import Control.Exception
import Control.Concurrent.Async.Lifted
import Text.Regex.TDFA
import Data.Char
import qualified Control.Concurrent as C
import qualified System.Directory as D
import qualified Data.Text.Lazy as LT
import qualified Data.Ord as O
import qualified Data.List as L
import qualified System.IO as IO
import qualified System.IO.Error as IO
import qualified Data.Text.Lazy.IO as TIO
default (T.Text)


multiVariant2Args :: Int -> [Text]
multiVariant2Args x
            | x == 1 = ["--term", "TIMEPOINTS"]
            | x == 2 = ["--term", "TIMESTAMPS"]
            | x == 3 = ["--term", "NO_TERM"]
            | x == 4 = ["--term", "NO_TERM", "-e"]
            | otherwise = error "multisource variant must be 1 to 4"

tt :: FilePath -> T.Text
tt = toTextIgnore

it :: (Show a, Integral a) => a -> T.Text
it = T.pack . show

ft :: (Show a, Real a) => a -> T.Text
ft = T.pack . show

launchReplayer :: Ctxt -> FilePath -> Sh (Async ())
launchReplayer c preProcessDir = do
        echo "Starting Replayer ..."
        asyncSh . print_stdout False $
            run_ (c^.replayer) replayerArgs
    where
        args = c^.inputArgs
        extraArgs = if (args^.usekafka)
            then ["-o", "kafka"]
            else ["-o", sformat (stext % ":" % int) (args^.sockhost) (args^.sockport) ]
        replayerArgs = ["-i", "csv", "-n", it $ args^.kafkaparts, "-a", ft $ args^.replayeraccel,
            "--clear", "--other_branch", "-t", "1000"] ++ (multiVariant2Args $ args^.multisourcevariant) ++ extraArgs ++ [tt $ preProcessDir]

launchFlinkImpl :: Ctxt -> FilePath -> FilePath -> Maybe FilePath -> Sh (Async ())
launchFlinkImpl c preProcessDir flinkOutDir savePointDir = do
        maybe (echo "Starting Flink ...") (\_ -> echo "Restarting Flink") savePointDir
        asyncSh . print_stdout False $
            run_ (c^.flinkExe) ("run" : restoreArgs ++ flinkArgs ++ kafkaReplayerArg ++ deciderArg)
    where
        args = c^.inputArgs
        restoreArgs = maybe [] (\p -> ["-s", tt $ p]) savePointDir
        flinkArgs = [tt $ c^.jarPath, "--format", "csv", "--sig", tt $ c^.sigFile, "--formula",
                tt $ c^.formulaFile, "--negate", "false", "--multi", it $ args^.multisourcevariant, "--monitor", "monpoly",
                "--processors", it $ args^.processors, "--out", tt flinkOutDir, "--nparts", it $ args^.kafkaparts, "--command", "\"/home/rofl/git/scalable-online-monitor/evaluation/run-monpoly.sh {ID}\""]
        kafkaReplayerArg = case ((args^.usereplayer), (args^.usekafka)) of
            (True, True) -> ["--in", "kafka", "--clear", "false"]
            (False, True) -> ["--in", "kafka", "--kafkatestfile", tt $ preProcessDir, "--clear", "true"]
            _ -> ["--in", sformat (stext % ":" % int) (args^.sockhost) (args^.sockport)]
        deciderArg = if args^.usedecider then ["-decider"]
                                         else []

launchFlink :: Ctxt -> FilePath -> FilePath -> Sh (Async ())
launchFlink c preProcessDir flinkOutDir = launchFlinkImpl c preProcessDir flinkOutDir Nothing

launchFlinkRestore :: Ctxt -> FilePath -> FilePath -> FilePath -> Sh (Async ())
launchFlinkRestore c preProcessDir flinkOutDir savePointDir = launchFlinkImpl c preProcessDir flinkOutDir (Just savePointDir)

restoreFlink :: Ctxt -> FilePath -> Sh (Async ())
restoreFlink c savePointDir = do
    echo "Restoring Flink ..."
    asyncSh . print_stdout False $
        run_ (c^.flinkExe) ["run", "-s", tt $ savePointDir]

cancelFlink :: Ctxt -> FilePath -> Sh (Async ())
cancelFlink c savePointDir = do
    echo "Canceling job and creating savepoint"
    asyncSh . print_stdout False $! do
        jobId <- getJobId c 
        cmd (c^.flinkExe) "cancel" jobId "-s" savePointDir

getJobId :: Ctxt -> Sh T.Text
getJobId c = do
        output <- print_stdout False $ cmd (c^.flinkExe) "list" "-r"
        let matches = getAllTextSubmatches (output =~ jobRegex) :: [T.Text]
        case compare (L.length matches) 2 of
            O.LT -> fail "no running job found"
            O.GT -> fail "too many jobs found"
            O.EQ -> return $! matches!!1
    where
        jobRegex = ": ([0-9a-z]+) : Parallel Online Monitor"

launchCheckpointingRun :: Ctxt -> Int -> FilePath -> FilePath -> FilePath -> Sh (Async ())
launchCheckpointingRun c sleepSecs savePointDir preProcessDir flinkOutDir = asyncSh $ do
        whenM (test_d savePointDir) $
            rm_rf savePointDir
        mkdir_p savePointDir
        replayerJob <- launchReplayer c preProcessDir
        flinkJob <- (errExit False) . (print_stderr False) $
            launchFlink c preProcessDir flinkOutDir
        sleepJob <- sleep sleepSecs & asyncSh
        result <- waitEither flinkJob sleepJob
        
        case result of
            Left _ -> do
                echo_err =<< lastStderr 
                fail "flink terminated before timeout expired"
            Right _ -> do
                cancelFlink c savePointDir >>= wait
                echo "Job was canceled"
                wait flinkJob
                metaDataFile <- findFile c savePointDir (Just "_metadata")
                case metaDataFile of
                    Just f -> do
                        newFlinkJob <- launchFlinkRestore c preProcessDir flinkOutDir f
                        void $! waitBoth replayerJob newFlinkJob
                    Nothing -> fail "couldn't find metadata file of the canceled flink job"


findFile :: Ctxt -> FilePath -> Maybe T.Text -> Sh (Maybe FilePath)
findFile c dir name =
    let nameArg = maybe [] (\s -> ["-name", s]) name in
        print_stdout False $ do
            files <- run "find" ([tt $ dir, "-type", "f"] ++ nameArg)
            let fLines = T.lines files
            if (L.length fLines < 1) then return Nothing
            else return . Just . fromText . T.strip $ (fLines!!0)

findFlinkOutFile :: Ctxt -> FilePath -> Sh (Maybe FilePath)
findFlinkOutFile c flinkOutDir = findFile c flinkOutDir Nothing

withFileSh :: FilePath -> IO.IOMode -> (IO.Handle -> Sh a) -> Sh a
withFileSh fp mode =
        bracket_sh (liftIO $! IO.openFile (f2s fp) mode)
         (liftIO . IO.hClose)
    where
        f2s = T.unpack . T.strip . toTextIgnore

fastPipe :: FilePath -> [T.Text] -> FilePath -> Sh ()
fastPipe cmd_fp args outPath = withFileSh outPath IO.WriteMode $ \h ->
    print_stdout False $ runHandle cmd_fp args $ \hOut ->
        liftIO $ TIO.hGetContents hOut >>= TIO.hPutStr h

withTmpDirOptional :: Bool -> (FilePath -> Sh a) -> Sh a
withTmpDirOptional noClear act = do
    trace "withTmpDir"
    dir <- liftIO D.getTemporaryDirectory
    tid <- liftIO C.myThreadId
    (pS, fhandle) <- liftIO $ IO.openTempFile dir ("tmp" ++ L.filter isAlphaNum (show tid))
    let p = fromText $ pack pS
    liftIO $ IO.hClose fhandle
    rm_f p
    mkdir p
    act p `finally_sh` (unless noClear $ rm_rf p)

main :: IO ()
main = do
    ctxt <- makeContext
    let args = ctxt^.inputArgs
    shelly . verbosely . print_commands False . errExit True . escaping False . (withTmpDirOptional (args^.noclear)) $ \tmpDir ->
        let logFile = tmpDir </> "trace.log"
            logFileCsv = tmpDir </> "trace.csv"
            referenceFile = tmpDir </> "reference.txt"
            flinkOutDir = tmpDir </> "flink-out"
            preProcessDir = tmpDir </> "preprocess_out"
            finalOutFile = tmpDir </> "out.txt"
            savePointDir = tmpDir </> "savepoints"
            genshapeArg = sformat ("-" % stext) (args^.generatorshape)
            genEasyArg = if args^.easy then ["-pA", ft $ (0.001 :: Float), "-pB", ft $ (0.001 :: Float)]
                         else []
            genExpArg = if not $ T.null (args^.exponents) then ["-z", args^.exponents]
                        else []
            transDistArg = if not $ T.null (args^.distribution) then ["--distribution", args^.distribution]
                         else []
        in
        do
            when (args^.noclear) $
                echo . sformat ("TMP dir is " % spath) $ tmpDir

            echo "Generating log ..."
            fastPipe (ctxt^.traceGenerator)
                ([genshapeArg, "-e", it $ args^.eventrate, "-i", it $ args^.indexrate, "-x", "1", it $ args^.loglength]
                    ++ genEasyArg ++ genExpArg)
                logFileCsv

            echo "Preprocessing for multiple inputs ..."
            run_ (ctxt^.traceTransformer) (["-v", (it $ args^.multisourcevariant), "-n", (it $ args^.kafkaparts),
                "-o", tt $ preProcessDir, "--max_ooo", (it $ args^.maxooo), "--sigma", (ft $ args^.sigma),
                "--watermark_period", (it $ args^.watermarkperiod), tt$ logFileCsv] ++ transDistArg)
            
            unless (args^.novalidate) $ do
                echo "Converting to monpoly format for validation ..."
                fastPipe (ctxt^.replayer)
                    ["-i", "csv", "-f", "monpoly", "-a", "0", tt logFileCsv]
                    logFile
                echo "Creating reference output ..."
                fastPipe (ctxt^.monpolyExe)
                    ["-sig", tt $ ctxt^.sigFile, "-formula", tt $ ctxt^.formulaFile, "-log", tt logFile]
                    referenceFile

            if args^.usereplayer then
                case args^.insertcheckpoint of
                    Just timeout ->     
                        wait =<< launchCheckpointingRun ctxt timeout savePointDir preProcessDir flinkOutDir
                    Nothing -> do
                        r <- launchReplayer ctxt preProcessDir
                        f <- launchFlink ctxt preProcessDir flinkOutDir
                        void $! waitBoth r f
            else
                wait =<< launchFlink ctxt preProcessDir flinkOutDir

            unless (args^.novalidate) $ do
                flinkOutFile <- findFlinkOutFile ctxt flinkOutDir
                reference <- readfile referenceFile
                if (T.null . T.strip $ reference) then do
                    echo "no violations in reference, bad test case"
                    echo "=== TEST PASSED ==="
                else case flinkOutFile of
                    Just f -> do
                        verdicts <- readfile f
                        echo "Comparing outputs ..."
                        let (correct, noReference, diffs) = verifyVerdicts (ctxt^.shouldCollapse) reference verdicts
                        echo $ sformat ("Verifying " % int % " verdicts ...") noReference
                        if correct then echo "=== TEST PASSED ==="
                        else do
                            echo diffs
                            echo "=== TEST FAILED ==="
                    Nothing -> fail "non empty reference but no flink output found"