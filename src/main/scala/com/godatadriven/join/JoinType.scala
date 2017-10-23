package com.godatadriven.join


sealed trait JoinType

final case class IterativeBroadcastJoinType(iterations: Int) extends JoinType

final case class SortMergeJoinType() extends JoinType


