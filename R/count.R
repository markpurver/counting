counts <- function(.data, ..., .drop = FALSE)
{
  # Error if dplyr not installed (or installed.packages fails)
  err <- character(0)
  if (tryCatch(!"dplyr" %in% rownames(installed.packages()), error=function(e){err <<- e$message; TRUE}))
    stop("You need to install the dplyr package in order to count data with this function - try running install.packages(\"dplyr\")\n", if (any(nchar(err)>0)) paste("\nProblem:", err), call.=FALSE)
  # .drop and equivalent drop_zeros argument made either TRUE or FALSE
  drop_zeros <- .drop <- tryCatch(if (any(!is.na(as.logical(unlist(.drop)[[1]])))) unlist(.drop)[[1]] else FALSE, error=function(e){FALSE})
  # Make .drop NULL if tidyr is needed to keep zero-count rows, and warn if zero-count rows cannot be kept
  if (installed.packages()["dplyr","Version"]<"0.8.0")
  {
    .drop <- NULL
    if (!drop_zeros && !"tidyr" %in% rownames(installed.packages()))
    {
      warning("Rows with a count of 0 are excluded from output because you do not have either the tidyr package or version 0.8.0 or above of the dplyr package - try running install.packages(\"tidyr\") or install.packages(\"dplyr\")\n", call.=FALSE, immediate.=TRUE)
      drop_zeros <- TRUE
    }
  }
  # Give a message (regardless of whether there was a warning) if zero-count rows are being dropped
  if (drop_zeros)
    message("Rows with a count of 0 are excluded from output\n")
  # Coerce input parameters
  err <- character(0)
  if (tryCatch(
    {
      if (!is.data.frame(.data))
        .data <- as.data.frame(.data)
      !is.data.frame(.data)
    },
    error=function(e){err <<- e$message; TRUE}
  ))
    stop("The data cannot be made into a data frame\n", if (any(nchar(err)>0)) paste("\nProblem: ", err), call.=FALSE)
  class_df <- class(.data)
  # Do some pure magic to evaluate unquoted arguments passed into the function
  dims <- as.list(match.call(expand.dots=FALSE)$`...`)
  # Names that need to be avoided in the output count data, because they are used unavoidably during processing
  protected_names <- unique(c("dplyr::n()"))
  if (length(dims)>0)
  {
    tryCatch(
      # Sort out dimension names (which will be output column names unless there is only one dimension) if necessary
      if (length(names(dims))==0)
        names(dims) <- sapply(dims,function(dm){sapply(attr(terms.formula(as.formula(paste("",as.expression(dm),sep="~"))), "variables"), deparse, width.cutoff=500L, backtick=FALSE)[-1]})
      else
        names(dims)[nchar(names(dims))==0] <- sapply(dims[nchar(names(dims))==0],function(dm){sapply(attr(terms.formula(as.formula(paste("",as.expression(dm),sep="~"))), "variables"), deparse, width.cutoff=500L, backtick=FALSE)[-1]}),
      error=function(e){stop("Illegal column name(s) - are you using backticks and backslashes where necessary (e.g. `a b` for column 'a b')?\n\nProblem: ", e$message, call.=FALSE)}
    )
    names(dims) <- names(suppressWarnings(suppressMessages(dplyr::as_tibble(setNames(dplyr::as_tibble(as.list(character(length(dims)+length(protected_names))), .name_repair="minimal"), c(protected_names,names(dims))), .name_repair="unique"))))[if (length(protected_names)>0) -(1:length(protected_names)) else rep(TRUE,length(dims))]
    nms <- setNames(as.list(character(length(dims))), names(dims))
    is_a_list <- setNames(logical(length(dims)), names(dims))
    # Make a data frame of all the necessary columns, hopefully making things faster and allowing names to be acquired using as.formula etc with 'backtick' either TRUE or FALSE
    .data <- tryCatch(
      dplyr::bind_cols(mapply(
        function(sets_call, dimname, .data)
        {
          sets <- as.expression(sets_call)
          while (is.expression(sets))
          {
            sets_expr <- sets
            sets <- eval(parse(text=sets), envir=.data)
          }
          if (is.list(sets) && length(sets)>0)
          {
            if (length(names(sets))==0)
              names(sets) <- sapply(as.call(parse(text=sets_expr))[[1]][1:length(sets)+1], function(set_call){sapply(attr(terms.formula(as.formula(paste("",as.character(parse(text=as.expression(set_call))),sep="~"))), "variables"), deparse, width.cutoff=500L, backtick=FALSE)[-1]})
            else
              names(sets)[nchar(names(sets))==0] <- sapply(as.call(parse(text=sets_expr))[[1]][(1:length(sets))[nchar(names(sets))==0]+1], function(set_call){sapply(attr(terms.formula(as.formula(paste("",as.character(parse(text=as.expression(set_call))),sep="~"))), "variables"), deparse, width.cutoff=500L, backtick=FALSE)[-1]})
            is_a_list[[dimname]] <<- TRUE
          }
          else
            sets <- setNames(list(sets), dimname)
          sets <- suppressWarnings(suppressMessages(dplyr::as_tibble(sets, .name_repair="unique")))
          nms[[dimname]] <<- sapply(attr(terms.formula(as.formula(sets)),"variables"), deparse, width.cutoff=500L, backtick=TRUE)[-1]
          sets
        },
        dims, names(dims), MoreArgs=list(.data), SIMPLIFY=FALSE
      )),
      error=function(e){stop("Column name(s) or function(s) not found - are you using backticks and backslashes where necessary (e.g. `a b` for column 'a b'), and have you loaded or referenced the packages for any functions used (e.g. use dplyr::case_when for the case_when function from dplyr, or run library(dplyr) first)?\n\nProblem: ", e$message, call.=FALSE)}
    )
  }
  else
  {
    names(.data) <- names(suppressWarnings(suppressMessages(dplyr::as_tibble(setNames(dplyr::as_tibble(as.list(character(length(.data)+length(protected_names))), .name_repair="minimal"), c(protected_names,names(.data))), .name_repair="unique"))))[if (length(protected_names)>0) -(1:length(protected_names)) else rep(TRUE,length(.data))]
    nms <- setNames(list(if (ncol(.data)>0) sapply(attr(terms.formula(as.formula(.data)),"variables"), deparse, width.cutoff=500L, backtick=TRUE)[-1]), "group")
    is_a_list <- TRUE
  }
  nms_cols <- lapply(1:length(nms), function(set_num,num_sets,nms_cols,is_a_list){c(if (is_a_list[set_num]) nms_cols[sum(is_a_list[1:set_num])+num_sets], nms_cols[set_num])}, length(nms), make.unique(c(names(nms), paste(names(nms)[is_a_list],"set",sep="_")), sep=".."), is_a_list)
  nms_grid <- expand.grid(nms,stringsAsFactors=FALSE)
  count_var <- make.unique(c(names(nms_grid), "count", sep=".."))[length(nms_grid)+1]
  # Make the table of counts
  add_na <- logical(length(unlist(nms_cols)))
  if (ncol(nms_grid)>0)
  {
    .data <- suppressWarnings(suppressMessages(do.call(
      if (prod(sapply(nms,length))==1) identity else rbind,
      lapply(
        1:prod(sapply(nms,length)),
        function(rownum, nms_grid, nms_cols, .data, count_var, .drop, drop_zeros)
        {
          counts_set <- dplyr::rename_at(
            dplyr::ungroup(dplyr::summarise(
              do.call(dplyr::group_by, c(list(.data=.data), unlist(mapply(function(group_column, nms_set, .data){setNames(list(addNA(eval(parse(text=group_column), envir=.data), ifany=TRUE), if (length(nms_set)>1) factor(sapply(attr(terms.formula(as.formula(paste("",group_column,sep="~"))),"variables"),deparse,width.cutoff=500L,backtick=FALSE)[-1]))[length(nms_set):1], nms_set)}, unname(nms_grid[rownum,,drop=FALSE]), nms_cols, MoreArgs=list(.data), SIMPLIFY=FALSE), recursive=FALSE), .drop=.drop)),
              dplyr::n()
            )),
            "dplyr::n()", ~count_var
          )
          # Record which columns need to have NA added to their levels (if rbind is being used)
          unlist(lapply(1:(length(counts_set)-1), function(colnum, counts_set){if (anyNA(counts_set[[colnum]]) || anyNA(levels(counts_set[[colnum]]))) add_na[colnum] <<- TRUE; NULL}, counts_set))
          # Use tidyr to add zero-count rows if the version of dplyr is too old
          if (is.null(.drop) && !drop_zeros)
          {
            set_levels <- lapply(1:(length(counts_set)-1), function(colnum, counts_set){levels(counts_set[[colnum]])}, counts_set)
            counts_set <- do.call(tidyr::complete, c(list(data=counts_set), lapply(names(counts_set)[-length(counts_set)],as.symbol), list(fill=setNames(list(0), count_var))))
            unlist(lapply(1:length(set_levels), function(colnum, set_levels){counts_set[[colnum]] <<- factor(counts_set[[colnum]], levels=set_levels[[colnum]], exclude=NULL); NULL}, set_levels))
          }
          counts_set
        },
        nms_grid, nms_cols, .data, count_var, .drop, drop_zeros
      )
    )))
    # Add NA level to columns if necessary after using rbind
    if (prod(sapply(nms,length))>1)
      unlist(lapply(1:length(add_na), function(colnum, add_na){if (add_na[colnum]) .data[[colnum]] <<- addNA(.data[[colnum]], ifany=FALSE); NULL}, add_na))
  }
  else
    .data <- dplyr::as_tibble(setNames(nm=as.list(c(unlist(nms_cols), count_var))))[FALSE,]
  # Return final data frame in class of original input
  if ("data.table" %in% class_df)
    data.table::as.data.table(.data)
  else if (!"tbl" %in% class_df)
    as.data.frame(.data)
  else
    .data
# Usage: counts_df <- counts(.data, ..., .drop = FALSE)
# This function takes line-by-line information within a data frame or something that can be made into a data frame (.data), and produces labelled counts of combinations of values in specified columns as a data frame of the same type, allowing multi-dimensional, multi-table and hierarchical counting
# All output columns except the counts column will be given as factors, with all input factor levels preserved and NA added as a level if there are any NA values (if there is hierarchical or multi-table counting, the order of the levels may not be preserved because levels will be combined); all NA values in an input column will be counted together regardless of whether they correspond to a factor level or not
# The counts column of the output will be called 'count' unless this clashes with another column name, in which it will be renamed to something like 'count..1'
# ... is a set of inputs specifying columns or functions of columns for which to count combinations within .data - this gives multi-dimensional counts in tidy form, with one dimension per input; ideally the inputs should have names giving the labels, and the inputs can be quoted or unquoted (e.g. x="as.character(a)" or x=as.character(a) to get column 'a' converted to characters and labelled as 'x') - you can't use a string to create an extra column containing that string on every row; entering any invalid inputs such as NULL or NA will usually cause an error
# You can use a variable containing column names, but they need to be evaluated or provided as expressions, e.g. if you have a variable called 'x' that contains "a+b", you need to give eval(parse(text=x)) to the function in order to get counts for column a plus column b of .data, or alternatively you can make the value of 'x' into expression(a+b) or expression("a+b") and then just provide x to the function; if you provide "a+b" directly it will work, but if you provide x when x is "a+b" it will be evaluated to the string "a+b" instead of the sum of columns a and b; you can provide a string via a variable containing "a+b" or directly using "\"a+b\"" if you really want to give a string rather than using .data
# Lists can be used as inputs to specify labels that should go in the same column, allowing hierarchies or multiple tables (in which case you will get a preceding column to label each hierarchy and table), e.g. j=list(x=a, b) or j="list(x=a, b)" to count in columns 'a' and 'b' with labels in column 'j', and with a preceding column 'j_set' labelling the sets as 'x' and 'b'
# You can create an input list with hierarchies using the 'counts_unpack' function with nested lists
# Input column names containing non-alphanumeric characters or starting with a number must be entered using ticks, whether quoted or not, e.g. column 'a b' should be entered as `a b` or "`a b`", and special symbols need to be preceded by two backslashes when quoted or one backslash when unquoted, e.g. column '`' should be entered as `\`` or "`\\``
# If you don't provide input names to be used when labelling, then output column names and some values will be assigned from the inputs, and this can sometimes give different names depending on whether the inputs are quoted or unquoted
# If only .data and .drop are provided, with no other inputs, counts will be provided for each column in the data frame individually (treating them as separate tables, not as dimensions); to instead use each column of data frame '.data' as a dimension, run: do.call(counts, c(list(.data), lapply(attr(terms.formula(as.formula(.data)), "variables"), deparse, width.cutoff=500L, backtick=TRUE)[-1]))
# .drop is either FALSE (default) to include output rows with a count of zero, or TRUE to exclude them; if FALSE, there will be a row for each unique combination of values and factor levels among the counted columns (including NA values and factor levels, which will not be distinguished from one another)
}

counts_unpack <- function(...)
{
  inputs_list2 <- as.list(match.call(expand.dots=FALSE)$`...`)
  print(inputs_list2)
  inputs_list <- list(...)
  # Initial value of output, only used if there is no meaningful input
  outputs_list <- character(0)
  if (length(unlist(inputs_list))>0)
  {
    # Internal function to go into lists recursively and bring together the same level for each one
    unpack_internal <- function(inputs, inputs_list_name)
    {
      inputs <- inputs[lapply(inputs, length)>0]
      names(inputs) <- make.unique(names(inputs), sep="..")
      outputs_list[length(outputs_list)+1] <<- paste0(inputs_list_name, if (length(inputs)>1) paste0("_",length(inputs)), " = dplyr::case_when(", paste0(lapply(inputs, function(input){paste0(unlist(input), collapse=" | ")}), " ~ ", ifelse(is.na(names(inputs)), "NA_character_", paste0("\"",gsub("\"","\\\"",names(inputs),fixed=TRUE),"\"")), collapse=(", ")), ")")
      if (any(unlist(lapply(inputs, function(input){is.list(input) || length(input)>1}))))
        unpack_internal(
          unlist(
            unname(mapply(
              function(input, inputs_name)
              {
                if (length(names(input))==0)
                  names(input) <- if (length(input)==1) inputs_name else input
                as.list(input)
              },
              inputs, names(inputs), SIMPLIFY=FALSE
            )),
            recursive=FALSE
          ),
          inputs_list_name
        )
    }
    unlist(mapply(
      function(inputs, inputs_list_name)
      {
        if (length(names(inputs))==0)
          names(inputs) <- if (length(inputs)==1) inputs_list_name else inputs
        unpack_internal(as.list(inputs), inputs_list_name)
        NULL
      },
      inputs_list, if (length(names(inputs_list))>0) names(inputs_list) else inputs_list, SIMPLIFY=FALSE
    ))
    outputs_list <- as.expression(paste0("list(\n  ", paste0(outputs_list, collapse=",\n  "), "\n", ")"))
  }
  print(outputs_list)
  outputs_list
}
# Blank names to be amended? Support unquoting? Error messages.
