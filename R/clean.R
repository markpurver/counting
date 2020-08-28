read <- function(path, ..., read_function = NULL, args_csv = list(), args_excel = list(), excel = c(NA, FALSE, TRUE), remote = c(NA, FALSE, TRUE), remote_function = s3tools::read_using)
{
  # Coerce input parameters
  path <- unlist(path)[[1]]
  MoreArgs <- list(...)
  args_csv <- as.list(args_csv)
  args_excel <- as.list(args_excel)
  tryCatch(
    {
      # Keep unpacking the first elements of read_function until you reach a function or error
      read_function <- unlist(list(eval(parse(text=expression(read_function)))))[[1]]
      while (!is.function(read_function) && any(nchar(read_function)) %in% TRUE)
        read_function <- unlist(list(eval(parse(text=read_function))))[[1]]
    },
    error=function(e)
    {
      stop(e$message, "\n  Could not find the read_function", call.=FALSE)
    }
  )
  excel <- unlist(excel)[[1]]
  remote <- unlist(remote)[[1]]
  # Decide what function to use to read the file specified in 'path', using the function given by read_function if it is not empty, or else using read_excel if 'excel' is TRUE, fread if it is FALSE, or using the file extension to decide if 'excel' is neither TRUE nor FALSE
  if (is.function(read_function))
  {
    package <- character(0)
    Args <- c(path, MoreArgs)
    df_function <- identity
  }
  else
    tryCatch(
      {
        if (excel %in% TRUE || !excel %in% FALSE && substr(path,regexpr("\\.[^\\.]*$",path),nchar(path)) %in% c(".xls",".xlt",".xla",".xlsx",".xlsm",".xlsb","xltx","xltm",".xlam",".xlw",".xlr",".xml",".ods"))
        {
          file_type <- "spreadsheet"
          package <- "readxl"
          read_function <- readxl::read_excel
          Args <- c(path, args_excel)
        }
        else
        {
          file_type <- "text"
          package <- "data.table"
          read_function <- data.table::fread
          Args <- c(path, args_csv)
        }
        Args <- c(Args, MoreArgs[names(MoreArgs) %in% names(formals(read_function))])
        df_function <- as.data.frame
      },
      error=function(e)
      {
        stop(e$message, "\n  You need to install the ", package, " package if you want to read ", file_type, " files without specifying the read_function to use - try running install.packages(\"", package, "\")", call.=FALSE)
      }
    )
  # Try to read the file locally if remote is FALSE, or from a remote source (e.g. S3 Bucket) if it is TRUE, or try both (locally first) if it is neither TRUE nor FALSE; only send arguments in ... with names that the chosen function accepts; give an error message if the file cannot be read
  tryCatch(
    if (!remote %in% TRUE)
      df_function(do.call(read_function, Args))
    else
      stop(),
    error=function(e)
    {
      if (!remote %in% FALSE)
      {
        tryCatch(
          {
            # Keep unpacking the first elements of remote_function until you reach a function or error
            remote_function <- unlist(list(eval(parse(text=expression(remote_function)))))[[1]]
            while (!is.function(remote_function) && any(nchar(remote_function)) %in% TRUE)
              remote_function <- unlist(list(eval(parse(text=remote_function))))[[1]]
            if (!is.function(remote_function))
              stop()
          },
          error=function(e2)
          {
            stop(if (!remote %in% TRUE) paste0(e$message, "\n  Failed to read file locally\n\n Error: "), e2$message, "\n  Failed to read file remotely: ", if (all(as.list(match.call(definition=sys.function(1),call=sys.call(1),expand.dots=FALSE))$remote_function=="s3tools::read_using")) "You need to install the s3tools package if you want to read files remotely from S3 Buckets without specifying the remote_function to use - try running install.packages(\"remotes\") followed by remotes::install_github(\"moj-analytical-services/s3tools\")" else "Could not find the remote_function", call.=FALSE)
          }
        )
        tryCatch(
          {
            if (is.function(remote_function))
              df_function(do.call(remote_function, c(read_function, Args)))
            else
              stop()
          },
          error=function(e2)
          {
            stop(if (!remote %in% TRUE) paste0(e$message, "\n  Failed to read file locally\n\n Error: "), e2$message, "\n  Failed to read file remotely", call.=FALSE)
          }
        )
      }
      else
        stop(e$message, "\n  Failed to read file locally", call.=FALSE)
    }
  )
# Usage: df <- read(path, ..., read_function=NULL, args_csv=list(), args_excel=list(), excel=c(NA, FALSE, TRUE), remote=c(NA, FALSE, TRUE), remote_function="s3tools::read_using")
# This function reads data from a file into a data frame using either 'data.table::fread' or 'readxl::read_excel' by default based on the file extension (in which case the resulting data table or tibble will be made into a simple data frame), or reads it using a function specified in 'FUN_string' if provided (in which case the output class will be unchanged, e.g. you will get a data table if you enter FUN_string="data.table::fread")
# The object given by 'path' will be the first input to whatever function is used to read the file (usually the file path, but can be something fancier when using 'fread'; the first input may have different names in different functions but that doesn't matter)
# The objects given by '...' will be any other arguments to whatever function is being used to read the file (NB: these don't always have the same names in different functions, so only those options that have the right names will be passed in - if you are calling this function repeatedly on a mixture of Excel and CSV files with the same options where names are the same, you can pass in the options for both types and they will be used appropriately, e.g. you can say which values should be changed to NA by passing in 'na' and 'na.strings' and it will be understood by 'read_excel', 'fread', 'read_csv' and 'read.csv', and you can pass in both 'header' and 'col_names' to specify whether a header row should be read in 'fread', 'read.csv', 'read_excel' and 'read_csv'; if read_function is not provided, partial matching is not allowed on ...)
# If 'read_function' is provided (as a function or string containing function name), then the function specified will be used to read the file (along with the arguments in ..., with partial matching allowed); if not, 'excel' will be used to determine whether to use 'read_excel' (if TRUE) or 'fread' (if FALSE), and if 'excel' is neither TRUE nor FALSE (NA is the default) then the file extension will be used to determine which of the two functions should be used
# If read_function is not provided, you can provide separate options for CSV and Excel files as lists using args_csv (for fread) and args_excel (for read_excel), with partial matching allowed, and these will be added to the appropriate options in ... (remember not to specify options in ... if they are in args_csv or args_excel)
# If 'remote' is TRUE, the 'remote_function' (default 's3tools::read_using') will be used to look for the file in a remote location (e.g. S3 Bucket) specified by 'path', assuming that the 'remote_function' takes the 'read_function' as its first argument and the 'path' as its second; if 'remote' is FALSE, the file will be sought locally using 'path'; if 'remote' is neither TRUE nor FALSE (NA is the default), a local file will be sought first, followed by a remote location if that fails
}

tidyup <- function(.data, header_names = ".", grep_header_names = TRUE, case_header_names = TRUE, all_header_names = TRUE, unique_headers = TRUE, clean_headers = FALSE, lower_case_headers = FALSE, na_strings = "", grep_na_strings = FALSE, case_na_strings = TRUE, remove_na_rows = TRUE, remove_na_cols = TRUE)
{
  # Coerce input parameters
  tryCatch(
    if (!is.data.frame(.data))
      .data <- as.data.frame(.data),
    error=function(e){stop("The data cannot be made into a data frame - ", e$message, call.=FALSE)}
  )
  grep_header_names <- !any(unlist(grep_header_names)[[1]] %in% FALSE)
  case_header_names <- !any(unlist(case_header_names)[[1]] %in% FALSE)
  fun_case_header_names <- if (grep_header_names || case_header_names) as.character else tolower
  header_names <- unique(fun_case_header_names(if (!grep_header_names || !all(unlist(header_names) %in% NA)) unlist(header_names) else NULL))
  all_header_names <- !any(unlist(all_header_names)[[1]] %in% FALSE)
  fun_all_header_names <- if (all_header_names) all else any
  unique_headers <- !any(unlist(unique_headers)[[1]] %in% FALSE)
  clean_headers <- any(unlist(clean_headers)[[1]] %in% TRUE)
  lower_case_headers <- any(unlist(lower_case_headers)[[1]] %in% TRUE)
  grep_na_strings <- any(unlist(grep_na_strings)[[1]] %in% TRUE)
  case_na_strings <- !any(unlist(case_na_strings)[[1]] %in% FALSE)
  fun_case_na_strings <- if (grep_na_strings || case_na_strings) as.character else tolower
  na_strings <- unique(fun_case_na_strings(unlist(na_strings)[!unlist(na_strings) %in% NA]))
  remove_na_rows <- !any(unlist(remove_na_rows)[[1]] %in% FALSE)
  remove_na_cols <- !any(unlist(remove_na_cols)[[1]] %in% FALSE)
  # Look for header names if requested, doing nothing if the header names are already in the data frame names
  if (length(header_names)>0 && (length(names(.data))==0 || !fun_all_header_names(if (grep_header_names) sapply(header_names[!header_names %in% NA],function(pattern,x,ignore.case){any(grepl(pattern,x,ignore.case))},x=names(.data),ignore.case=!case_header_names) else header_names %in% fun_case_header_names(names(.data)))))
  {
    # Go through each row of the data frame looking for the header patterns or names (case-sensitive or case-insensitive depending on case_header_names)
    header_row <- 0
    row_count <- 1
    if (ncol(.data)>0)
      while (header_row==0 && row_count<=nrow(.data))
      {
        print(sapply(header_names[!header_names %in% NA],function(pattern,x,ignore.case){any(grepl(pattern,x,ignore.case))},x=unlist(.data[row_count,]),ignore.case=!case_header_names))
        if (fun_all_header_names(if (grep_header_names) sapply(header_names[!header_names %in% NA],function(pattern,x,ignore.case){any(grepl(pattern,x,ignore.case))},x=unlist(.data[row_count,]),ignore.case=!case_header_names) else header_names %in% fun_case_header_names(unlist(.data[row_count,]))))
          header_row <- row_count
        row_count <- row_count + 1
      }
    # If a header row is found, apply the header names to the data frame and remove all rows from the header row upwards
    if (header_row>0)
    {
      # Warn if the headers are on the last row
      if (header_row==nrow(.data))
        warning("Returning data frame with zero rows - header row found on the last row by a case-", if (case_header_names) "in", "sensitive ", "search for a row containing ", if (all_header_names) "all" else "any", " of the ", if (grep_header_names) "patterns" else "strings", " in header_names\n", call.=FALSE, immediate.=TRUE)
      # Apply correct headers and remove any data above the header row
      .data <- suppressWarnings(setNames(.data[-(1:header_row),,drop=FALSE], unlist(lapply(.data[header_row,], as.character))))
    }
    else
      # Warn if the requested headers are absent, and return data frame with unchanged headers
      warning("Returning data frame with unchanged headers - header row not found by a case-", if (case_header_names) "in", "sensitive ", "search for a row containing ", if (all_header_names) "all" else "any", " of the ", if (grep_header_names) "patterns" else "strings",  " in header_names\n", call.=FALSE, immediate.=TRUE)
  }
  # If requested,
  if (unique_headers || clean_headers || lower_case_headers)
  {
    if (is.null(names(.data)))
      names(.data) <- rep("", ncol(.data))
    names2 <- names(.data)
    if (clean_headers)
      names2 <- gsub("[^[:alnum:]]", "_", make.names(names2))
    if (lower_case_headers)
      names2 <- tolower(names2)
    if (unique_headers)
    {
      o <- order(tolower(names(.data))==names2)
      names2[o] <- make.unique(names2[o], sep="_")
    }
    names(.data) <- names2
  }
  # If requested, make any values found in na_strings into NA within the data frame using a string or pattern search (case-sensitive or case-insensitive depending on case_na_strings), without changing column types within the data frame; this is not applied to column names as they are after any search for the header row (fread and read.csv also don't change headers to NA, but read_excel and read_csv do)
  if (length(na_strings)>0 && nrow(.data)>0 && ncol(.data)>0)
    lapply(1:ncol(.data), function(column,na_strings){.data[[column]][if (grep_na_strings) apply(sapply(na_strings,grepl,x=.data[[column]],ignore.case=!case_na_strings), 1, any) else fun_case_na_strings(.data[[column]]) %in% na_strings] <<- NA; NULL}, na_strings)
  # If requested, remove any rows containing nothing but NA after applying na_strings (not applied to column names as they are after any search for the header row)
  if (remove_na_rows && nrow(.data)>0)
    .data <- .data[apply(.data, 1, function(vals){!all(vals %in% NA)}),,drop=FALSE]
  # If requested, remove any columns containing nothing but NA after applying na_strings (the thing with the two dots is a silly fix to allow data.table objects to be processed, because otherwise the data table would look for a column called 'columns' within itself instead of a variable outside)
  if (remove_na_cols && ncol(.data)>0)
  {
    columns <- ..columns <- which(unlist(lapply(.data, function(vals){!all(vals %in% NA)})))
    .data <- .data[,..columns]
  }
  # Return the final data frame
  .data
# Usage: .data <- tidyup(.data, header_names=NULL, case_header_names=TRUE, all_header_names=TRUE, na_strings="", case_na_strings=TRUE, remove_na_rows=TRUE, remove_na_cols=TRUE)
# This function attempts simple steps to tidy up any object (.data) that is, or can be made into, a data frame (e.g. data frame, data table, tibble or matrix) by looking for a header row (which requires some header names and so is not done by default), making header names unique and valid for R and SQL (without needing to protect the names), creating NA values and removing empty rows and columns; objects which are a sub-class of data frame retain all their classes (so tibbles remain tibbles, but matrices become data frames)
# If 'header_names' is specified, it should be a vector of header patterns or names in the file you are reading (depending on 'grep_header_names' - this is TRUE by default, in which case the pattern will be sought as a regular expression using the 'grep' function, but if FALSE only the exact string will be sought); if you provide something in 'header_names', the existing data frame names or else the first row containing either all or any of the header patterns or names (depending on 'all_header_names'), found from either a case-sensitive or case-insensitive search (depending on case_header_names), will be taken as the header row, with everything above that row ignored; the default is to use the data frame names or first row that contains any non-blank values (i.e. anything except "" and NA)
# After looking for headers, 'unique_headers' will determine whether header names should be made unique, 'clean_headers' will determine whether they should be made valid for R and SQL without needing to protect the names (FALSE by default) and lower_case_headers will determine whether they should be made lower-case (FALSE by default)
# After tidying up headers, data frame values or patterns found in 'na_strings' will be made into NA (depending on 'grep_na_strings', which is FALSE by default), using either a case-sensitive or case-insensitive string comparison (depending on 'case_na_strings'); if 'grep_na_strings' is FALSE, only exact values matching any of the values in 'na_strings' will be made into NA, but if it TRUE then the 'grep' function will be used to convert values matching any of the patterns in 'na_strings'; 'na_strings' will not affect header names, which mimics the behaviour of 'fread' and 'read.csv' but not of 'read_excel' and 'read_csv'; the default value of 'na_strings' is the same as in 'read_excel', which is different to the other aforementioned functions; to avoid creating any new NA values, set na_strings to NULL (or any other zero-length object) or NA
# After setting NA values, 'remove_na_rows' and 'remove_na_columns' will determine whether to remove rows and columns containing only NA values (NA values in a factor count as NA whether or not they have a level); this is not affected by whether a row or column has a name, or what that name is
# When utilisting grep (if grep_header_names or grep_na_strings is TRUE), remember that you can't search for NA (but you can match it if they are FALSE), and also remember that you can surround text with \Q and \E if you want it to be interpreted literally, e.g. "\Q*\E" will look for strings containing a star instead of interpreting the star as a wildcard character in a regular expression
}
