# sparkz
An extension to the amazing Spark framework for better functional programming.
The project aims to extend, and in a few cases re-implement, some of the functionalities and classes in the [Apache Spark](spark.apache.org) framework.

The main motivation is to make statically typed the APIs of some ML componenets avoiding to use DataFrame, to provide the missing functional structures of some components (Broadcast variables, data validation pipelines, utility classes...) and to work around the unnecessary limitations imposed by private fields/methods.
Moreover, the project introduces a bunch of util functions, implicits and tutorials to show the power, conciseness and elegance of the Spark framework when combined with a fully functional design.

Current features:

* Functional Data Validation using monads and applicative functors: https://datasciencevademecum.wordpress.com/2016/03/09/functional-data-validation-using-monads-and-applicative-functors/
* Integration with sceval for a better binary classification evaluation framework: https://github.com/samthebest/sceval
* Immutable StatCounter class with defined Monoid
* Collection of Pimps for daily tasks utils

WIP:
* Functor for Spark Broadcast
* Transformer -> Trainer -> Model -> Evaluation functional framework for machine learning algorithms (similar to ML pipeline but typed and functional)

Related projects:
* Frameless: https://github.com/adelbertc/frameless/
