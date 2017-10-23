package com.godatadriven.join


sealed trait JoinType

final case class IterativeBroadcastJoinType() extends JoinType

final case class SortMergeJoinType() extends JoinType


