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

#' Read a file on HDFS into a data frame.
#'
#' \code{read_hdfs} reads a file on HDFS into a data frame
#'
#' This function reads a file on HDFS into a data frame. By default, it will use
#' \code{read_delim} in \code{readr} to read the file. If no delimiter is specified
#' and \code{read_delim} is used, it will use a comma as the delimiter.
#'
#' @param path File path on HDFS of the file to read in.
#' @param read_fn Function to use to read the file (\code{readr::read_delim} by default)
#' @param ... Additional parameters to pass to \code{read_fn}
#' @param file_argnum The position of the file path argument in \code{read_fn}
#' (e.g., \code{file} is the first argument in \code{readr::read_delim},
#' \code{path} is the first argument in \code{readxl::read_excel})
#'
#' @return The object read by \code{read_fn}.
#' @export
read_hdfs <- function(path, read_fn = readr::read_delim, ..., file_argnum = 1) {
  if (Sys.getenv("USE_KERBEROS") == "1") {
    check_kerberos()
  }
  rhdfs::hdfs.init()
  tmp <- tempfile(fileext = paste0(".", tools::file_ext(path)))
  on.exit(suppressWarnings(file.remove(tmp)))
  rhdfs::hdfs.get(path, tmp)

  read_args <- lazyeval::lazy_dots(...)
  if (identical(read_fn, readr::read_delim) && !purrr::contains(names(read_args), "delim")) {
    read_args <- lazyeval::lazy_dots(delim = ",", ...)
  }
  read_args <- purrr::map(read_args, lazyeval::lazy_eval)
  file_arg <- names(formals(read_fn))[[file_argnum]]
  read_args[[file_arg]] <- tmp
  purrr::lift_dl(read_fn)(read_args)
}

#' Write a data frame to a file on HDFS.
#'
#' @param df A data frame.
#' @param path File path on HDFS to write to
#' @param write_fn Function to use to write the data frame to a file (\code{readr::write_csv} by default)
#' @param ... Additional paramters to pass to \code{write_csv}
#' @param df_argnum The position of the data frame argument in \code{write_fn}
#' (e.g., \code{x} is the first argument in \code{readr::write_csv},
#' \code{object} is the first argument in \code{saveRDS})
#' @param file_argnum The position of the file path argument in \code{write_fn}
#' (e.g., \code{path} is the second argument in \code{readr::write_csv}),
#' \code{file} is the second argument in \code{saveRDS}
#'
#' @export
write_hdfs <- function(df, path, write_fn = readr::write_delim, ...,
                       df_argnum = 1, file_argnum = 2) {
  if (Sys.getenv("USE_KERBEROS") == "1") {
    check_kerberos()
  }
  rhdfs::hdfs.init()
  tmp <- tempfile()
  on.exit(suppressWarnings(file.remove(tmp)))

  write_args <- lazyeval::lazy_dots(...)
  if (identical(write_fn, readr::write_delim) && !purrr::contains(names(write_args), "delim")) {
    write_args <- lazyeval::lazy_dots(delim = ",", ...)
  }
  write_args <- purrr::map(write_args, lazyeval::lazy_eval)
  df_arg <- names(formals(write_fn))[[df_argnum]]
  file_arg <- names(formals(write_fn))[[file_argnum]]
  write_args[[file_arg]] <- tmp
  write_args[[df_arg]] <- df
  purrr::lift_dl(write_fn)(write_args)
  rhdfs::hdfs.put(tmp, path)
}


