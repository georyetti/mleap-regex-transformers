# mleap-regex-transformers
## Overview
This repository serves as an example of how to build MLEAP custom transformers, following the guide given in the [MLEAP repository](https://combust.github.io/mleap-docs/mleap-runtime/custom-transformer.html). 

It implements common regex operations such as:

- **EXTRACT** - Extracts the first string in the input that matches the expression and corresponding to the group index.
- **EXTRACT ALL** - Extracts all strings in the str that match the expression and corresponding to the group index.
- **LIKE** - Returns true if the input string matches the expression, or false otherwise.
- **REPLACE** - Replaces all substrings of the input that match the expression with a given string.

## Running the project

Install Java 11 and Scala 2.12.15.

To compile the project, run in your terminal:
```sbt clean compile```

To run the sample project which demonstrates the functionality, run in your terminal:
```sbt run```

## As dependency
If you want to use the library as a dependency to another project you can do, though it is not published to central Maven. 

To do so, clone the repository and run `sbt publishLocal`. 
This will publish the project jar to your local ivy repository from which you can then include `georyetti:mleap-regex-transformers_2.12:0.0.1` as a dependency in another local project.

Within your other project, you can then import Spark transformers, use them in pipelines, and seralize them into MLEAP bundles using the below:
```
import org.apache.spark.ml.mleap.transformer.RegexExtract
import georyetti.regex.mleap.model.RegexExtractModel

val regexTransformer = new RegexExtract(model = RegexExtractModel(regexString = "<REGEX_PATTERN>", idx = <GROUP_TO_EXTRACT>))
      .setInputCol("<STRING_COLUMN_NAME_TO_APPLY_REGEX>")
      .setOutputCol("<OUTPUT_COLUMN_NAME")
```

See the [sample project](src/main/scala/Sample.scala) for more examples. 

### License
Released under MIT license.
