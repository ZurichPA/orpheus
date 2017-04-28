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

#' Connect to a Hive database
#'
#' \code{hive_connect} opens a connection to a Hive database
#'
#' @param db Name of the Hive database to connect to
#' @param identifier_quote Identifier quote character
#' @param hive_driver Hive driver class
#' @param parameters String of parameters to use in initializing the JVM
#' @param hadoop_env List of Hive parameters to set
#' @param default_pars A list of Hive parameters being set by default
#' @return A JDBCConnection object to the Hive database
#' @export
hive_connect <- function(db,
                         identifier_quote = "`",
                         hive_driver = "org.apache.hive.jdbc.HiveDriver",
                         parameters = "-Xmx2g",
                         hadoop_env = list(),
                         default_pars = list(
                           hive.vectorized.execution.enabled = "true",
                           hive.vectorized.execution.reduce.enabled = "true",
                           hive.resultset.use.unique.column.names = "false",
                           hive.tez.container.size = "5120",
                           hive.tez.java.opts = "-Xmx4096m"
                         )) {

  if (Sys.getenv("USE_KERBEROS") == "1") {
    check_kerberos()
  }

  rhdfs::hdfs.init()

  jar_folders <- stringr::str_split(Sys.getenv("HIVE_JAR_FOLDERS"), ":")[[1]]
  jar_list <- purrr::map(jar_folders, ~ list.files(., pattern = "\\.jar$", full.names = TRUE))
  jars <- purrr::flatten_chr(jar_list)
  rJava::.jinit(classpath = jars, parameters = parameters)

  hive_pars <- append(default_pars[setdiff(names(default_pars), names(hadoop_env))], hadoop_env)
  connection_vars <- paste(paste(names(hive_pars), unname(hive_pars), sep = "="), collapse = ";")

  jdbc_string <- paste0("jdbc:hive2://", Sys.getenv("HIVE_SERVER_HOST"), ":",
                        Sys.getenv("HIVE_SERVER_PORT"), "/", db)

  if (Sys.getenv("USE_KERBEROS") == "1") {
    jdbc_string <- paste0(jdbc_string, ";principal=hive/", Sys.getenv("HIVE_SERVER_HOST"),
                          "@", Sys.getenv("KERBEROS_REALM"))
  }

  jdbc_string <- paste0(jdbc_string, "?", connection_vars)

  dbConnect(JDBC(hive_driver, identifier.quote = identifier_quote), jdbc_string)
}

#' Execute a Hive query
#'
#' \code{hive_query} executes a SQL query on a Hive database and pulls down the result
#' to a local data frame.
#'
#' @param hive_con A \code{JDBCConnection} object created by \code{hive_connect}
#' @param query A string of the SQL query to run
#' @param clean_names Whether to remove database and table name from column names
#' @param batch Number of rows to fetch in each batch
#' @param update_every How often to print progress updates (after what number of batches)
#' @param max_rows The maximum number of rows to fetch from the result
#' @param quiet Whether or not to print progress updates
#' @export
hive_query <- function(hive_con, query, clean_names = TRUE, batch = 100000,
                       update_every = 1, max_rows = Inf, quiet = FALSE) {
  res <- dbSendQuery(hive_con, query)
  res <- as(res, "HiveResult")

  result <- vector(mode = "list", length = 100)
  row_cnt <- 0
  i <- 1

  while (row_cnt < max_rows) {
    chunk <- dbFetch(res, n = min(batch, max_rows, max_rows - row_cnt))
    gc()

    if (nrow(chunk) == 0L) break
    result[[i]] <- chunk
    row_cnt <- row_cnt + nrow(result[[i]])

    if (!quiet && ((i %% update_every) == 0)) {
      message("Rows processed: " , formatC(row_cnt, format = "f", big.mark = ",", digits = 0))
    }

    if (i %% 100 == 0) {
      result <- c(result, vector(mode = "list", length = 100))
    }
    i <- i + 1
  }

  dbClearResult(res)
  rJava::.jcall("java/lang/System", method = "gc")

  df <- dplyr::bind_rows(result)

  if (clean_names) {
    names(df) <- purrr::map_chr(stringr::str_split(names(df), stringr::fixed(".")), dplyr::last)
  }

  df
}


