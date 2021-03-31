# Writing TSDB – Example

This small program uses Prometheus TSDB Library to generate single TSDB block.

It is meant to work as an example to show how to
1) Generate chunks from some input data,
2) write chunks to disk using chunks writer,
3) how to generate index, adding symbols and series,
4) and write meta.json file.

Running this program generates TSDB block, which can then be stored into Prometheus and queried.

In practice you usually want to include many series, which complicates the process slightly.
Since all symbols must be written to the index in advance, this means that labels for all
series must be known before index can be written.

For real-world example, you can check out https://github.com/cortexproject/cortex/blob/484455f76785b19f976c376436b783aa49d1a806/tools/blocksconvert/builder/tsdb.go,
which was used to convert Cortex chunks to TSDB blocks.
