# Copyright 2017 Zurich
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' @import methods
#' @import RJDBC
#' @import DBI
NULL

#' HiveConnection class
#'
#' This is a Hive connection class that inherits from
#' \code{\link[RJDBC]{JDBCConnection-class}}, which
#' inherits from \code{\link[DBI]{DBIConnection-class}}
#'
#' There are custom \code{HiveConnection} methods for \code{dbSendQuery}
#' and \code{dbDataType}
#'
#' @export
setClass("HiveConnection", contains = "JDBCConnection")

#' HiveResult class
#'
#' This is a Hive result class that inherits from
#' \code{\link[RJDBC]{JDBCResult-class}}, which inherits
#' from \code{\link[DBI]{DBIResult-class}}
#'
#' There is a custom \code{HiveResult} method for \code{dbFetch}
#'
#' @export
setClass("HiveResult", contains = "JDBCResult")

#' @rdname HiveConnection-class
#'
#' @export
setMethod(
  "dbSendQuery",
  signature = c(conn = "HiveConnection", statement = "character"),
  function(conn, statement, ..., list = NULL) {
    res <- dbSendQuery(as(conn, "JDBCConnection"), statement)
    as(res, "HiveResult")
  }
)

#' @rdname HiveResult-class
#'
#' @export
setMethod(
  "dbFetch",
  signature = c(res = "HiveResult"),
  function(res, n = -1, ...) {
    dat <- dbFetch(as(res, "JDBCResult"), n)
    hive_types <- as.character(dbColumnInfo(res)[["field.type"]])
    r_types <- dplyr::recode(hive_types,
                             double = "as.double",
                             int = "as.integer",
                             string = "as.character",
                             boolean = "as.logical",
                             date = "as.Date",
                             timestamp = "as.POSIXct",
                             .default = "identity")
    r_type_list <- purrr::map(as.list(r_types), lazyeval::lazy_eval)
    dat_fmt <- purrr::map2(dat, r_type_list, ~ .y(.x))
    names(dat_fmt) <- names(dat)
    tibble::as_tibble(dat_fmt)
  }
)

#' @rdname HiveConnection-class
#'
#' @export
setMethod(
  "dbDataType",
  signature = "HiveConnection",
  function(dbObj, obj, ...) {
    switch(
      class(obj)[[1]],
      character = "STRING",
      Date =    "DATE",
      factor =  "STRING",
      integer = "INT",
      logical = "BOOLEAN",
      numeric = "DOUBLE",
      POSIXct = "TIMESTAMP",
      raw = "BINARY",
      stop(
        "Can't map",
        paste(class(obj), collapse = "/"),
        "to a supported type"))
  }
)
