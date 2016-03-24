package sparkz.examples

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Interval, LocalDate}
import sparkz.utils.Pimps._

import scalaz.Scalaz._
import scalaz.ValidationNel

case class UserEvent(userId: Long, eventCode: Int, timestamp: Long)

sealed trait InvalidEventCause
case object NonRecognizedEventType extends InvalidEventCause
case object BlackListUser extends InvalidEventCause
case object NonEligibleUser extends InvalidEventCause
case object OutOfGlobalIntervalEvent extends InvalidEventCause
case object FirstDayToConsiderEvent extends InvalidEventCause

case class InvalidEvent(event: UserEvent, cause: InvalidEventCause)

case object DataValidation {
  def validationFunction(events: RDD[UserEvent],
                         eligibleUsers: Set[Long],
                         validEventCodes: Set[Int],
                         blackListEventCodes: Set[Int],
                         minDate: String, maxDate: String): UserEvent => ValidationNel[InvalidEvent, UserEvent] = {
    val sc = events.context

    val validEventCodesBV: Broadcast[Set[Int]] = sc.broadcast(validEventCodes)
    val notRecognizedEventCode: PartialFunction[UserEvent, InvalidEventCause] = {
      case event if !validEventCodesBV.value.contains(event.eventCode) => NonRecognizedEventType
    }

    val eligibleUsersBV: Broadcast[Set[Long]] = sc.broadcast(eligibleUsers)
    val customerNotEligible: PartialFunction[UserEvent, InvalidEventCause] = {
      case event if !eligibleUsersBV.value.contains(event.userId) => NonEligibleUser
    }

    val blackListEventCodesBV: Broadcast[Set[Int]] = sc.broadcast(blackListEventCodes)
    // Users for which we observed a black list event
    val blackListUsersBV: Broadcast[Set[Long]] = sc.broadcast(
      events.filter(event => blackListEventCodesBV.value.contains(event.eventCode))
      .map(_.userId).distinct().collect().toSet
    )
    val customerIsInBlackList: PartialFunction[UserEvent, InvalidEventCause] = {
      case event if blackListUsersBV.value.contains(event.userId) => BlackListUser
    }


    val eventIsOutOfGlobalInterval: PartialFunction[UserEvent, InvalidEventCause] = {
      case event if !new Interval(DateTime.parse(minDate), DateTime.parse(maxDate)).contains(event.timestamp) =>
        OutOfGlobalIntervalEvent
    }

    // max between first date we have ever seen a customer event and the global min date
    val customersFirstDayToConsiderBV: Broadcast[Map[Long, LocalDate]] =
      sc.broadcast(
        events.keyBy(_.userId)
        .mapValues(personalEvent => new DateTime(personalEvent.timestamp).toLocalDate)
        .reduceByKey((date1, date2) => List(date1, date2).minBy(_.toDateTimeAtStartOfDay.getMillis))
        .mapValues(firstDate => List(firstDate, LocalDate.parse(minDate)).maxBy(_.toDateTimeAtStartOfDay.getMillis))
        .collect().toMap
      )
    val eventIsFirstDayToConsider: PartialFunction[UserEvent, InvalidEventCause] = {
      case event if customersFirstDayToConsiderBV.value(event.userId).isEqual(event.timestamp.toLocalDate) =>
        FirstDayToConsiderEvent
    }
    val validationRules: List[PartialFunction[UserEvent, InvalidEventCause]] =
      List(customerNotEligible, notRecognizedEventCode, customerIsInBlackList,
        eventIsOutOfGlobalInterval.orElse(eventIsFirstDayToConsider)
      )

    (event: UserEvent) => validationRules.map(_.toFailureNel(event, InvalidEvent(event, _))).reduce(_ |+++| _)
  }

  def onlyValidEvents(events: RDD[UserEvent],
                      validationFunc: UserEvent => ValidationNel[InvalidEvent, UserEvent]): RDD[UserEvent] =
    events.map(validationFunc).flatMap(_.toOption)

  def invalidEvents(events: RDD[UserEvent],
                    validationFunc: UserEvent => ValidationNel[InvalidEvent, UserEvent]): RDD[InvalidEvent] =
    events.map(validationFunc).flatMap(_.swap.toOption).flatMap(_.toList)

  def outOfRangeEvents(events: RDD[UserEvent],
                       validationFunc: UserEvent => ValidationNel[InvalidEvent, UserEvent]): RDD[UserEvent] =
    events.map(validationFunc).flatMap(_.swap.toOption).flatMap(_.toSet).flatMap {
      case InvalidEvent(event, OutOfGlobalIntervalEvent) => event.some
      case _ => Nil
    }

  // This method will return something like:
  // Map(Set(NonEligibleCustomer, NonRecognizedEventType) -> 36018450,
  // Set(NonEligibleUser) -> 9037691,
  // Set(NonEligibleUser, BlackListUser, NonRecognizedEventType) -> 137816,
  // Set(NonEligibleUser) -> 464694973,
  // Set(BeforeFirstDayToConsiderEvent, NonRecognizedEventType) -> 5147475,
  // Set(OutOfGlobalIntervalEvent, NonRecognizedEventType) -> 983478)
  def causeSetToInvalidEventsCount(events: RDD[UserEvent],
                                   validationFunc: UserEvent => ValidationNel[InvalidEvent, UserEvent]): Map[Set[InvalidEventCause], Int] =
    events.map(validationFunc)
    .map(_.swap).flatMap(_.toOption).map(_.map(_.cause).toSet -> 1)
    .reduceByKey(_ + _)
    .collect().toMap

  // This method will return something like:
//   Map(Set(NonEligibleCustomer, NonRecognizedEventType) -> 1545,
//   Set(NonEligibleUser) -> 122,
//   Set(NonEligibleUser, BlackListUser, NonRecognizedEventType) -> 3224,
//   Set(NonEligibleUser) -> 4,
//   Set(BeforeFirstDayToConsiderEvent, NonRecognizedEventType) -> 335,
//   Set(OutOfGlobalIntervalEvent, NonRecognizedEventType) -> 33)
  def causeSetToUsersLostCount(events: RDD[UserEvent],
                               validationFunc: UserEvent => ValidationNel[InvalidEvent, UserEvent]): Map[Set[InvalidEventCause], Int] = {
    val survivedUsersBV: Broadcast[Set[Long]] =
      events.context.broadcast(events.map(validationFunc).flatMap(_.toOption).map(_.userId).distinct().collect().toSet)

    events.map(validationFunc).flatMap(_.swap.toOption)
    .keyBy(_.head.event.userId)
    .filter(_._1 |> (!survivedUsersBV.value(_)))
    .mapValues(_.map(_.cause).toSet)
    .mapValues(Set(_))
    .reduceByKey(_ ++ _)
    .flatMap(_._2)
    .map(_ -> 1)
    .reduceByKey(_ + _)
    .collect().toMap
  }
}
