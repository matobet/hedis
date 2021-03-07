{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE PatternSynonyms #-}

module Database.Redis.Protocol (Reply(..), pattern BlobString, pattern ArrayReply, reply, renderRequest) where

import Prelude hiding (error, map, null, take)
#if __GLASGOW_HASKELL__ < 710
import Control.Applicative
#endif
import Control.DeepSeq
import Scanner (Scanner)
import qualified Scanner
import Data.ByteString.Char8 (ByteString)
import GHC.Generics
import qualified Data.ByteString.Char8 as B
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Read as Text
import Control.Monad (replicateM)

-- |Low-level representation of replies from the Redis server.
data Reply = SingleLine ByteString
           | Error ByteString
           | Integer Integer
           | Bulk (Maybe ByteString)
           | MultiBulk (Maybe [Reply])
           | Null
           | Double Double
           | Boolean Bool
           | BulkError ByteString
           | MapReply [(Reply, Reply)]
           | SetReply [Reply]
           | Push [Reply]
         deriving (Eq, Show, Generic)

instance NFData Reply

pattern BlobString :: ByteString -> Reply
pattern BlobString x <- Bulk (Just x) where
  BlobString x = Bulk (Just x)

pattern ArrayReply :: [Reply] -> Reply
pattern ArrayReply x <- MultiBulk (Just x) where
  ArrayReply x = MultiBulk (Just x)

------------------------------------------------------------------------------
-- Request
--
renderRequest :: [ByteString] -> ByteString
renderRequest req = B.concat (argCnt:args)
  where
    argCnt = B.concat ["*", showBS (length req), crlf]
    args   = renderArg <$> req

renderArg :: ByteString -> ByteString
renderArg arg = B.concat ["$",  argLen arg, crlf, arg, crlf]
  where
    argLen = showBS . B.length

showBS :: (Show a) => a -> ByteString
showBS = B.pack . show

crlf :: ByteString
crlf = "\r\n"

------------------------------------------------------------------------------
-- Reply parsers
--
-- >>> Scanner.scanOnly reply "+hello world\r\n"
-- Right (SingleLine "hello world")
--
-- >>> Scanner.scanOnly reply "-MYERROR some fail\r\n"
-- Right (Error "MYERROR some fail")
--
-- >>> Scanner.scanOnly reply ":1\r\n"
-- Right (Integer 1)
--
-- >>> Scanner.scanOnly reply "(42\r\n"
-- Right (Integer 42)
--
-- >>> Scanner.scanOnly reply "$11\r\nhello world\r\n"
-- Right (Bulk (Just "hello world"))
--
-- >>> Scanner.scanOnly reply "*3\r\n+foo\r\n*1\r\n:42\r\n$5\r\nhello\r\n"
-- Right (MultiBulk (Just [SingleLine "foo",MultiBulk (Just [Integer 42]),Bulk (Just "hello")]))
--
-- >>> Scanner.scanOnly reply "_\r\n"
-- Right Null
--
-- >>> Scanner.scanOnly reply "$-1\r\n"
-- Right (Bulk Nothing)
--
-- >>> Scanner.scanOnly reply "*-1\r\n"
-- Right (MultiBulk Nothing)
--
-- >>> Scanner.scanOnly reply "%0\r\n"
-- Right (MapReply [])
--
-- >>> Scanner.scanOnly reply "%2\r\n+foo\r\n(100\r\n+bar\r\n~1\r\n+qux\r\n"
-- Right (MapReply [(SingleLine "foo",Integer 100),(SingleLine "bar",SetReply [SingleLine "qux"])])
{-# INLINE reply #-}
reply :: Scanner Reply
reply = do
  c <- Scanner.anyChar8
  case c of
    '+' -> string
    '-' -> error
    ':' -> integer -- TODO: Make Int64
    '(' -> integer -- Big integer
    '$' -> bulk
    '*' -> multi
    '_' -> null
    ',' -> double
    '#' -> boolean
    '=' -> bulk
    '%' -> map
    '~' -> set
    '>' -> push
    _ -> fail "Unknown reply type"

{-# INLINE string #-}
string :: Scanner Reply
string = SingleLine <$> line

{-# INLINE error #-}
error :: Scanner Reply
error = Error <$> line

{-# INLINE integer #-}
integer :: Scanner Reply
integer = Integer <$> integral

double :: Scanner Reply
double = Double <$> floating

boolean :: Scanner Reply
boolean = Boolean <$> bool

{-# INLINE bulk #-}
bulk :: Scanner Reply
bulk = Bulk <$> do
  len <- integral
  if len < 0
    then return Nothing
    else Just <$> Scanner.take len <* eol

-- don't inline it to break the circle between reply and multi
{-# NOINLINE multi #-}
multi :: Scanner Reply
multi = MultiBulk <$> do
  len <- integral
  if len < 0
    then return Nothing
    else Just <$> replicateM len reply

null :: Scanner Reply
null = Null <$ eol

map :: Scanner Reply
map = MapReply <$> replicateIntegral mapEntry

mapEntry :: Scanner (Reply, Reply)
mapEntry = (,) <$> reply <*> reply

set :: Scanner Reply
set = SetReply <$> replicateIntegral reply

push :: Scanner Reply
push = Push <$> replicateIntegral reply

{-# INLINE integral #-}
integral :: Integral i => Scanner i
integral = line >>= decodeText (Text.signed Text.decimal)

replicateIntegral :: Scanner a -> Scanner [a]
replicateIntegral s = integral >>= (`replicateM` s)

inf :: Double
inf = 1 / 0

negInf :: Double
negInf = -1 / 0

floating :: Scanner Double
floating = do
  str <- line
  if | str == "inf" -> return inf
     | str == "-inf" -> return negInf
     | otherwise -> decodeText Text.double str

decodeText :: Text.Reader a -> ByteString -> Scanner a
decodeText r str = case r (Text.decodeUtf8 str) of
  Left err -> fail (show err) 
  Right (l, _) -> return l 

bool :: Scanner Bool
bool = do
  str <- line
  if | str == "t" -> return True
     | str == "f" -> return False
     | otherwise -> fail "Expected `t` or `f` as boolean value"

{-# INLINE line #-}
line :: Scanner ByteString
line = Scanner.takeWhileChar8 (/= '\r') <* eol

{-# INLINE eol #-}
eol :: Scanner ()
eol = do
  Scanner.char8 '\r'
  Scanner.char8 '\n'
