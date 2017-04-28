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

#' Kerberos check
#'
#' \code{check_kerberos} check to see if the Kerberos ticket can
#' be refreshed. If not, it will throw an error.
#'
#' @export
check_kerberos <- function() {
  status_code <- system2("kinit", "-R", stdout = FALSE, stderr = FALSE)
  if (status_code != 0) stop("Unable to renew Kerberos ticket")
  invisible(status_code)
}

