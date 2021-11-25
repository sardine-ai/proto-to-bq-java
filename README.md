# proto-to-bq-java

Convert protobuf java object to bigquery's TableRow

Example usage:

```java
ParDo.of(new DoFn<YourDataClass, TableRow>() {
  @DoFn.ProcessElement
  public void processElement(ProcessContext c) {
    c.output(ProtoToBigQuery.toTableRow(c.element()));
  }
})
```

Note you need to re-deploy your dataflow job whenever proto schema changes

LICENSE: MIT
