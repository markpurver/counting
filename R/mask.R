suppress <- function(.data, group_cols = NULL, set_cols = NULL, ignore_if = NULL, suppress_if = NULL, suppress_group_if = NULL, suppress_while = NULL, suppress_order = NULL, groups_equivalent_if = NULL, suppress_equivalent_while = suppress_while)
{
  # Make the data into a data frame if it isn't already
  tryCatch(
    if (!is.data.frame(.data))
      .data <- as.data.frame(.data),
    error=function(e){stop("The data cannot be made into a data frame - ", e$message, call.=FALSE)}
  )
  # Make the data required for suppression, in case functions of columns are specified (do this before re-ordering anything, in case it's a string specifying a vector of numbers); if group_cols and/or set_cols is empty, make a dummy column for them so that secondary suppression assumes everything to be part of one group and/or set
  tryCatch(
    {
      # Get group and set information, and sort group_cols to make the suppression deterministic, regardless of the input order of group column names (the order in which group columns, or dimensions, are suppressed, affects the output mask; there may be a different result if the column names are changed so that they have a different alphabetical order)
      group_cols <- sort(unique(unlist(group_cols)))
      group_set_cols <- unique(c(group_cols, sort(unlist(set_cols))))
      group_set_vars <- if (length(group_set_cols)>0) lapply(paste0("~", group_set_cols), function(col){all.vars(as.formula(col))}) else list()
      group_set_data <- with(.data, as.data.frame(lapply(group_set_cols, function(col){eval(parse(text=as.character(col)))}), stringsAsFactors=FALSE))
      if (length(group_set_cols)==0)
      {
        group_set_data <- data.frame(rep(NA,nrow(.data)), rep(NA,nrow(.data)))
        group_cols <- NA
      }
      else if (length(group_cols)==0)
      {
        group_set_data <- cbind(data.frame(rep(NA,nrow(.data))), group_set_data)
        group_cols <- NA
      }
      else if (length(group_set_cols)==length(group_cols))
      {
        group_set_data <- cbind(group_set_data, .data.frame(rep(NA,nrow(.data))))
      }
      if (nrow(group_set_data)!=nrow(.data))
        stop("Neither group_cols nor set_cols gives the same number of rows as the data", call.=FALSE)
    },
    error=function(e){stop("Either group_cols or set_cols cannot be made from the data - ", e$message, call.=FALSE)}
  )
  # Make a single secondary suppression ordering column from the data, giving everything equal precedence if it has not been specified, and the same for the suppression order of equivalent groups (do this before re-ordering anything, in case it's a string specifying a vector of numbers)
  tryCatch(
    {
      suppression_order <- with(.data, as.data.frame(lapply(suppress_order, function(col){eval(parse(text=as.character(col)))}), stringsAsFactors=FALSE))
      suppression_order <- if (prod(dim(suppression_order))>0) suppression_order[rep_len(1:nrow(suppression_order),nrow(.data)),,drop=FALSE] else data.frame(rep_len(NA, nrow(.data)))
      suppress_order <- 1:nrow(.data)
      suppress_order[do.call(order, suppression_order)] <- suppress_order
    },
    error=function(e){stop("The suppress_order sequence cannot be made from the data - ", e$message, call.=FALSE)}
  )
  # Order the data and suppression order by all columns to make the suppression deterministic, no matter the initial data order (there may be a different result if the column names are changed so that they have a different alphabetical order)
  overall_order <- do.call(order, .data)
  .data <- .data[overall_order,,drop=FALSE]
  group_set_data <- group_set_data[overall_order,,drop=FALSE]
  suppress_order <- suppress_order[overall_order]
  # Make a mask showing which rows in the data should be ignored (FALSE means they can't be suppressed, TRUE means they can)
  mask_ignore <- tryCatch(if (any(nchar(trimws(unlist(ignore_if)[[1]])))>0) rep_len(with(.data, !as.logical(eval(parse(text=unlist(ignore_if)[[1]])))), nrow(.data)) else rep_len(TRUE, nrow(.data)), error=function(e){stop("The ignore_if condition is not valid for the data - ", e$message, call.=FALSE)})
  # Get the non-ignored rows corresponding to equivalent groups (equivalent_group_rows) and the columns that equivalent rows have in common (equivalent_group_by), ensuring deterministic output by sorting the equivalent groups alphabetically, first sorting each list element, and then sorting the list by the first part of each element
  tryCatch(
    {
      groups_equivalent_if <- lapply(groups_equivalent_if, function(equivalent_groups){sort(trimws(unlist(equivalent_groups)[!unlist(equivalent_groups) %in% NA & nchar(trimws(unlist(equivalent_groups)))>0]))})
      groups_equivalent_if <- groups_equivalent_if[sapply(groups_equivalent_if,length)>0]
      if (length(groups_equivalent_if)>0)
        groups_equivalent_if <- groups_equivalent_if[order(unlist(lapply(groups_equivalent_if, function(equivalent_groups){equivalent_groups[1]})))]
      equivalent_groups_rows <- with(.data[mask_ignore,,drop=FALSE], lapply(groups_equivalent_if, function(equivalent_groups) {lapply(unlist(equivalent_groups), function(equivalent_group){which(as.logical(eval(parse(text=equivalent_group))))})}), stringsAsFactors=FALSE)
      equivalent_group_bys <- lapply(groups_equivalent_if, function(equivalent_groups, group_set_vars){which(unlist(lapply(group_set_vars, function(group_set_var, equivalent_vars){!any(group_set_var %in% equivalent_vars)}, unlist(lapply(unlist(equivalent_groups), function(equivalent_group){all.vars(as.formula(paste0("~", equivalent_group)))})))))}, group_set_vars)
    },
    error=function(e){stop("Some of the groups_equivalent_if conditions are not valid the data - ", e$message, call.=FALSE)}
  )
  # Create the logical mask (and a comparison version to check changes made by secondary suppression) by doing primary suppression on the non-ignored data (TRUE means not suppressed, FALSE means suppressed and NA can be used to mean uncertain)
  mask <- !mask_ignore | tryCatch(if (any(nchar(trimws(unlist(suppress_if)[[1]])))>0) rep_len(with(.data, !as.logical(eval(parse(text=unlist(suppress_if)[[1]])))), nrow(.data)) else rep_len(TRUE, nrow(.data)), error=function(e){stop("The suppress_if condition is not valid for the data - ", e$message, call.=FALSE)})
  # Update the mask to include secondary suppression by looping through each set of equivalent groups (hierarchies) and of groups (dimensions) in the non-ignored data, until the mask no longer changes
  if (length(suppress_order)>0 && ncol(group_set_data)>0 && nrow(group_set_data[mask_ignore,,drop=FALSE])>1)
  {
    # Function to do secondary suppression (if there has already been some primary suppression) of non-suppressed values using the suppress_while condition and in the order specified by group_suppress_order, or the nearest one to the suppressed value in the event of a tie (e.g. if suppress_order given as NA); note that there may already be more than one value suppressed, but further ones may be required to prevent someone narrowing down the set of possible values for the suppressed value(s)
    secondary_suppression <- function(group_mask, group_data, group_suppress_order, suppress_while)
    {
      # This has two parts in order to handle NA values if they arise (although hopefully they don't), as these could be turned into either TRUE or FALSE when the mask is used
      if (any(group_mask %in% FALSE))
      {
        group_order <- do.call(order, data.frame(group_suppress_order, abs(1:length(group_mask)-match(FALSE,group_mask))))
        while (any(group_mask %in% c(NA,TRUE)) && any(na.omit(with(group_data[group_mask %in% FALSE,,drop=FALSE], as.logical(eval(parse(text=suppress_while)))))))
          group_mask[group_order][na.omit(match(c(NA,TRUE), group_mask[group_order]))[1]] <- FALSE
      }
      if (any(group_mask %in% c(NA,FALSE)))
      {
        group_order <- do.call(order, data.frame(group_suppress_order, abs(1:length(group_mask)-match(NA,group_mask))))
        while (any(group_mask %in% TRUE) && any(na.omit(with(group_data[group_mask %in% c(NA,FALSE),,drop=FALSE], as.logical(eval(parse(text=suppress_while)))))))
          group_mask[group_order][match(TRUE, group_mask[group_order])] <- NA
      }
      group_mask
    }
    tryCatch(
      {
        suppress_group_if <- trimws(unlist(suppress_group_if)[[1]])
        suppress_while <- trimws(unlist(suppress_while)[[1]])
        suppress_equivalent_while <- trimws(unlist(suppress_equivalent_while)[[1]])
        if (any(nchar(suppress_while))>0)
        {
          #main_groups_rows <- rep(list(list(which(mask_ignore))), length(group_cols))
          main_groups_rows <- rep(list(list(1:sum(mask_ignore))), length(group_cols))
          main_group_bys <- lapply(1:length(group_cols), function(group_col, col_nums){col_nums[-group_col]}, 1:length(group_set_cols))
        }
        # Assemble a list of rows for each group within the data
        groups_list <- mapply(
          function(all_groups_rows, all_group_bys, group_set_data)
          {
            unlist(mapply(
              function(all_group_rows, all_group_by, group_set_data)
              {
                if (length(all_group_by)==0)
                  group_set_data[[all_group_by <- length(group_set_data)+1]] <- 1
                if (length(all_group_rows)>1)
                  all_group_rows <- unlist(lapply(1:(length(all_group_rows)-1), function(idx1, all_group_rows){lapply((idx1+1):length(all_group_rows), function(idx2, idx1, all_group_rows) {sort(unique(c(all_group_rows[[idx1]], all_group_rows[[idx2]])))}, idx1, all_group_rows)}, all_group_rows), recursive=FALSE)
                unlist(lapply(
                  all_group_rows,
                  function(all_group_row, all_group_by, group_set_data)
                  {
                    if (length(all_group_row)>1)
                    {
                      group_set_data <- group_set_data[all_group_row,,drop=FALSE]
                      ord <- do.call(order, group_set_data[all_group_by])
                      group_set_data <- group_set_data[all_group_by][ord,,drop=FALSE]
                      group_combos <- unique(group_set_data)
                      groups_list <- rep(list(1:ceiling(nrow(group_set_data)/length(group_combos))), length(group_combos))
                      group_start_row <- combo_row <- 1
                      unlist(lapply(
                        1:(nrow(group_set_data)-1),
                        function(group_end_row, group_combos, group_set_data)
                        {
                          if (!all(mapply(function(a,b){a %in% b}, group_set_data[group_end_row+1,,drop=FALSE], group_combos[combo_row,,drop=FALSE])))
                          {
                            groups_list[[combo_row]] <<- all_group_row[ord[group_start_row:group_end_row]]
                            group_start_row <<- group_end_row + 1
                            combo_row <<- combo_row + 1
                          }
                          NULL
                        },
                        group_combos, group_set_data
                      ))
                      if (group_start_row<nrow(group_set_data))
                        groups_list[[combo_row]] <- all_group_row[ord[group_start_row:nrow(group_set_data)]]
                    }
                    else
                      groups_list <- rep(list((1)[length(all_group_row)]), 1)
                    groups_list
                  },
                  all_group_by, group_set_data
                ), recursive=FALSE)
              },
              all_groups_rows, all_group_bys, MoreArgs=list(group_set_data)
            ), recursive=FALSE)
          },
          list(if (any(nchar(suppress_while))>0) main_groups_rows, if (any(nchar(suppress_equivalent_while))>0) equivalent_groups_rows), list(if (any(nchar(suppress_while))>0) main_group_bys, if (any(nchar(suppress_equivalent_while))>0) equivalent_group_bys), MoreArgs=list(group_set_data[mask_ignore,,drop=FALSE]), SIMPLIFY=FALSE
        )
        if (any(nchar(suppress_group_if))>0 || length(groups_list[[1]]>0 || length(groups_list[[2]]>0)))
        {
          # Make a mask of non-ignored data
          mask_current <- mask[mask_ignore]
          # Suppress whole groups if requested (excluding combined equivalent groups)
          if (any(nchar(suppress_group_if))>0)
            unlist(lapply(
              groups_list[[1]],
              function(group_list, .data)
              {
                mask_current[group_list] <<- mask_current[group_list] & with(.data[group_list,,drop=FALSE], !any(as.logical(eval(parse(text=unlist(suppress_group_if)[[1]])))))
                NULL
              },
              .data[mask_ignore,,drop=FALSE]
            ))
          while_conds <- c(rep(suppress_while, length(groups_list[[1]])), rep(suppress_equivalent_while, length(groups_list[[2]])))
          groups_list <- unlist(groups_list, recursive=FALSE)
          # If any secondary suppression is required, loop through all groups until no further changes to the mask are being made (looping through groups and combined equivalent groups)
          if (length(groups_list)>0)
          {
            mask_previous <- mask_current
            mask_changed <- TRUE
            while (mask_changed)
            {
              # Suppress values as required in each group, starting with the group whose first value in line for possible suppression is furthest down the order (i.e. go in reverse order among the first-in-line values, or take the last in line from the first-in-lines)
              groups_order <- rev(order(unlist(lapply(groups_list, function(group_list, suppress_order){suppressWarnings(min(suppress_order[group_list][mask_current[group_list] %in% c(NA,TRUE)]))}, suppress_order))))
              unlist(mapply(
                function(group_rows, while_cond, suppress_order, .data)
                {
                  if (length(group_rows)>1)
                    mask_current[group_rows] <<- secondary_suppression(mask_current[group_rows], .data[group_rows,,drop=FALSE], suppress_order[group_rows], while_cond)
                  NULL
                },
                groups_list[groups_order], while_conds[groups_order], MoreArgs=list(suppress_order[mask_ignore], .data[mask_ignore,,drop=FALSE])
              ))
              print(sum(!(is.na(mask_current)&is.na(mask_previous) | !is.na(mask_current)&!is.na(mask_previous)&mask_current==mask_previous)))
              if (!all(is.na(mask_current)&is.na(mask_previous) | !is.na(mask_current)&!is.na(mask_previous)&mask_current==mask_previous))
                mask_previous <- mask_current
              else
                mask_changed <- FALSE
            }
          }
          # Incorporate the mask of the non-ignored data into the overall mask
          mask[mask_ignore] <- mask_current
        }
      },
      error=function(e){stop("Either the suppress_group_if condition, the suppress_while condition or the suppress_equivalent_while condition is not valid for the data - ", e$message, call.=FALSE)}
    )
  }
  # Put the mask back into the original order of the input data, so they match
  mask[overall_order] <- mask
  mask
}
# CHECK THAT EQUIVALENT GROUPS ADD UP?; GROUP_COLS WITH GROUPS OF COLUMNS?; WHAT HAPPENS TO .data WITH A SINGLE GROUPING COLUMN?; STOP AFTER ONE ITERATION OF THE SECONDARY SUPPRESSION WHILE LOOP IF THERE IS ONLY ONE DIMENSION (AND ALSO NO EQUIVALENT GROUPS?)
