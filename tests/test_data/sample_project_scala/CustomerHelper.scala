package com.company.jobs

import org.apache.spark.sql.Row

object CustomerHelper {

  // BUG: does not null-check row.getAs[String]("email") before calling
  // .trim() on it - throws NullPointerException for any record with a
  // missing email.
  def validate(row: Row): Boolean = {
    val email = row.getAs[String]("email")
    email.trim().nonEmpty
  }
}
