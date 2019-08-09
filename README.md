# kdb-remote-batch
Execute batch with remote kdb process

## Build
Build with maven


## How to use
* Args
    * -c (--connect) VAL : kdb handle - `:host:port(:id:pw) (default: `::5001)
    * -f (--file) VAL    : script list file
    * -q (--qscript) VAL : q script file
    * -h (--help)        : show usage message and exit (default: false)

* Example 

```
java -jar -c `::5001 -q sample/test.q
java -jar -c `::5001 -f sample/test_files.list
```