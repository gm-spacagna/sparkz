# sparkz
[![Build Status](https://travis-ci.org/gm-spacagna/sparkz.svg?branch=master)](https://travis-ci.org/gm-spacagna/sparkz)

A proof-of-concept extension to the amazing Spark framework for better functional programming.
The project aims to extend, and in a few cases re-implement, some of the functionalities and classes in the [Apache Spark](spark.apache.org) framework.

The main motivation is to make statically typed the APIs of some Machine Learning components, to provide the missing functional structures of some classes (Broadcast variables, data validation pipelines, utility classes...) and to work around the unnecessary limitations imposed by private fields/methods.
Moreover, the project introduces a bunch of util functions, implicits and tutorials to show the power, conciseness and elegance of the Spark framework when combined with a fully functional design.

## Sonatype dependency
Maven:

    <dependency>
      <groupId>com.github.gm-spacagna</groupId>
      <artifactId>sparkz_2.10</artifactId>
      <version>0.1.0</version>
    </dependency>

sbt:

    "com.github.gm-spacagna" % "sparkz_2.10" % "0.1.0"

## Current features

* Functional Data Validation using monads and applicative functors: https://datasciencevademecum.wordpress.com/2016/03/09/functional-data-validation-using-monads-and-applicative-functors/
* Integration with sceval for a better binary classification evaluation framework: https://github.com/samthebest/sceval
* Immutable StatCounter class with defined Monoid
* Collection of Pimps for daily tasks utils
* Lazy logger for debug computations
* Transformer -> Trainer -> Model -> Evaluation functional framework for machine learning algorithms (similar to ML pipeline but typed and functional): https://datasciencevademecum.wordpress.com/2016/04/12/robust-and-declarative-machine-learning-pipelines-for-predictive-buyin/

## WIP
* Functor for Spark Broadcast

 
## Limitations
The original Spark implementations are intentionally not fully functional in order to avoid overloading the garbage collector and have more efficient and mutable data structures. This project is only a proof-of-concept with the goal of inspiring developers, data scientists and engineers to think their design in pure functional terms but does not guarantee better performances. It is strongly encouraged to tailor and tune each component based on your speficif needs.

## Related projects
* Frameless: https://github.com/adelbertc/frameless
* Exploratory Data Analysis: https://github.com/vicpara/exploratory-data-analysis
