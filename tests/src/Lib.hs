{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE BangPatterns #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module Lib
    ( verifyVerdicts,
      spath
    ) where

import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Read as TR
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.List as L
import qualified Data.Ord as O
import qualified Formatting as F
import qualified Debug.Trace as T
import Text.Regex.TDFA
import Formatting.Internal(later)
import Prelude hiding (FilePath)
import Shelly(FilePath, toTextIgnore)
default (T.Text)

data Verdict = Verdict {timepoint :: Int, timestamp :: Int, args :: [Int]} deriving (Eq, Ord, Show)

spath :: F.Format r (FilePath -> r)
spath = later (TB.fromText . toTextIgnore)

forceIntConversion :: T.Text -> Int
forceIntConversion = forceConversion . TR.decimal
  where
    forceConversion (Left a) = error $ "failed to convert " ++ a
    forceConversion (Right (a, _)) = a

parseReferenceLineArgs :: T.Text -> [[Int]]
parseReferenceLineArgs lineArgs =
    let regexTuples = "\\([0-9,]+\\)" :: T.Text
        regexVals = "[0-9]+"
        tups = getAllTextMatches (lineArgs =~ regexTuples) :: [T.Text]
        tupVals = map (\x -> getAllTextMatches (x =~ regexVals) :: [T.Text]) tups
    in
      map (\x -> map forceIntConversion x) tupVals

parseReference :: Bool -> T.Text -> [Verdict]
parseReference collapse reference =
    let ls = filter (not . T.null) (map T.strip (T.lines reference))
        regex_re = "^@([0-9]+). \\(time point ([0-9]+)\\): (.+)$"
        regex_re_parse = map (\l -> getAllTextSubmatches (l =~ regex_re) :: [T.Text]) ls
        args_parsed = T.trace ("regex_re_parse is " ++ (show regex_re_parse)) $
          map(\k -> (forceIntConversion (k!!1), forceIntConversion(k!!2), parseReferenceLineArgs (k!!3))) regex_re_parse
    in
      concatMap (\(ts, tp, a) -> map (\a ->
        let tp' = if collapse then ts else tp in
        Verdict {timepoint = tp', timestamp = ts, args = a}) a) args_parsed

verifyVerdicts :: Bool -> T.Text -> T.Text -> (Bool, T.Text)
verifyVerdicts collapse reference verdicts =
  let referenceParsed = L.sort $ parseReference collapse reference
      verdictsParsed = L.sort $ parseReference collapse verdicts
      diffLines = aux referenceParsed verdictsParsed (TB.fromLazyText "")
      diffLines' = TL.toStrict $ TB.toLazyText diffLines
  in
    (T.null diffLines', diffLines')
  where
    added k = (TB.fromLazyText "ADDED   ") <>  (TB.fromString $ show k) <> (TB.fromLazyText "\n")
    missing k = (TB.fromLazyText "MISSING   ") <>  (TB.fromString $ show k) <> (TB.fromLazyText "\n")
    aux (r : rs) (v : vs) !acc =
      case compare v r of
        O.LT -> aux (r : rs) vs ((added v) <> acc)
        O.GT -> aux rs (v : vs) ((missing r) <> acc)
        O.EQ -> aux rs vs acc
    aux [] (v : vs) !acc = aux [] vs ((added v) <> acc)
    aux (r: rs) [] !acc =  aux rs [] ((missing r) <> acc)
    aux [] [] !acc = acc