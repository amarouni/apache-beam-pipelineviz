# Apache Beam pipeline Graphviz generator
Generate a PNG with the Graphviz of your Apache Beam pipeline

# Usage
Build your Beam pipeline :
```java

PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
Pipeline p = Pipeline.create(options);

        Create.Values<String> input = Create.of(Arrays.asList("AZERTY""QWERTY";

        PCollection<KV<String, Long>> extractedWords = p
                .apply(input)
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement ...
                }))
                .apply(Count.<String>perElement());

        // First branch
        extractedWords
                .apply("FormatResults_001", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    ...
                })).apply("FormatResults_XXX_001", MapElements.via(new SimpleFunction<String, String>() {
            ...
        }));

        // Second branch
        extractedWords
                .apply("FormatResults_002", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    ...
                })).apply("FormatResults_XXX_002", MapElements.via(new SimpleFunction<String, String>() {
            ...
        }));

``` 

Call the GraphVizVisitor `writeGraph()` :
```java
GraphVizVisitor graphVizVisitor = new GraphVizVisitor(p, "/tmp/mypipeviz");
graphVizVisitor.writeGraph();
```

# Output
A PNG with Graphviz of your Beam pipeline :
![](assests/mypipeviz.png)