#' get_coord_shares
#'
#' Given a dataset of CrowdTangle shares and a time threshold, this function detects networks of entities (pages, accounts and groups) that performed coordinated link sharing behavior
#'
#' @param ct_shares.df the data.frame of link posts resulting from the function get_ctshares
#' @param coordination_interval a threshold in seconds that defines a coordinated share. Given a dataset of CrowdTangle shares, this threshold is automatically estimated by the estimate_coord_interval interval function. Alternatively it can be manually passed to the function in seconds
#' @param parallel enables parallel processing to speed up the process taking advantage of multiple cores (default FALSE). The number of cores is automatically set to all the available cores minus one
#' @param percentile_edge_weight defines the percentile of the edge distribution to keep in order to identify a network of coordinated entities. In other terms, this value determines the minimum number of times that two entities had to coordinate in order to be considered part of a network. (default 0.90)
#' @param clean_urls clean the URLs from the tracking parameters (default FALSE)
#' @param keep_ourl_only restrict the analysis to ct shares links matching the original URLs (default=FALSE)
#' @param gtimestamps add timestamps of the fist and last coordinated shares on each node. Slow on large networks (default=FALSE)
#'
#' @return A list (results_list) containing four objects: 1. The input data.table (ct_shares.dt) of shares with an additional boolean variable (coordinated) that identifies coordinated shares, 2. An igraph graph (highly_connected_g) with networks of coordinated entities whose edges also contains a t_coord_share attribute (vector) reporting the timestamps of every time the edge was detected as coordinated sharing, 3. A dataframe with a list of coordinated entities (highly_connected_coordinated_entities) with respective name (the account url), number of shares performed, average subscriber count, platform, account name, if the account name changed, if the account is verified, account handle, degree and component number
#'
#' @examples
#' output <- get_coord_shares(ct_shares.df)
#'
#' output <- get_coord_shares(ct_shares.df = ct_shares.df, coordination_interval = coordination.interval, percentile_edge_weight=0.9, clean_urls=FALSE, keep_ourl_only=FALSE, gtimestamps=FALSE)
#'
#' # Extract the outputs
#' get_outputs(output)
#'
#' # Save the data frame of CrowdTangle shares marked with the “is_coordinated” column
#' write.csv(ct_shares_marked.df, file=“ct_shares_marked.df.csv”)
#'
#' # Save the graph in a Gephi readable format
#' library(igraph)
#' write.graph(highly_connected_g, file="highly_connected_g.graphml", format = "graphml")
#'
#' # Save the data frame with the information about the highly connected coordinated entities
#' write.csv(highly_connected_coordinated_entities, file=“highly_connected_coordinated_entities.csv”)
#'
#' @importFrom stats quantile
#' @importFrom utils setTxtProgressBar txtProgressBar
#' @importFrom dplyr mutate mutate select filter
#' @importFrom parallel detectCores makeCluster stopCluster
#' @importFrom foreach foreach %dopar%
#' @importFrom doSNOW registerDoSNOW
#' @importFrom tidytable unnest bind_rows
#' @importFrom CooRTweet detect_groups
#' @importFrom CooRTweet generate_coordinated_network
#'
#' @export

get_coord_shares <- function(ct_shares.df, coordination_interval = NULL,
                             parallel = FALSE, percentile_edge_weight = 0.90,
                             clean_urls = FALSE, keep_ourl_only = FALSE,
                             gtimestamps = FALSE) {

  options(warn=-1)

  # estimate the coordination interval if not specified by the users
  if(is.null(coordination_interval)){
    coordination_interval <- estimate_coord_interval(ct_shares.df,
                                                     clean_urls = clean_urls,
                                                     keep_ourl_only = keep_ourl_only)
    coordination_interval <- coordination_interval[[2]]
  }

  # use the coordination interval resulting from estimate_coord_interval
  if(is.list(coordination_interval)){
    coordination_interval <- coordination_interval[[2]]
  }

  # use the coordination interval set by the user
  if(is.numeric(coordination_interval)){
    if (coordination_interval == 0) {
      stop("The coordination_interval value can't be 0.
           \nPlease choose a value greater than zero or use coordination_interval=NULL to automatically calculate the interval")
    } else {

    coordination_interval <- paste(coordination_interval, "secs")

    if (file.exists("log.txt")) {
      write(paste("\nget_coord_shares script executed on:", format(Sys.time(), format = "%F %R %Z"),
                  "\ncoordination interval set by the user:", coordination_interval), file="log.txt", append=TRUE)
    } else {
      write(paste0("#################### CooRnet #####################\n",
                   "\nScript executed on:", format(Sys.time(), format = "%F %R %Z"),
                  "\ncoordination interval set by the user:", coordination_interval),
            file="log.txt")
      }
    }
  }

  # keep original URLs only?
  if(keep_ourl_only==TRUE){
    ct_shares.df <- subset(ct_shares.df, ct_shares.df$is_orig==TRUE)
    if (nrow(ct_shares.df) < 2) stop("Can't execute with keep_ourl_only=TRUE. Not enough posts matching original URLs")
    write("Analysis performed on shares matching original URLs", file = "log.txt", append = TRUE)
  }

  # clean urls?
  if(clean_urls==TRUE){
    ct_shares.df <- clean_urls(ct_shares.df, "expanded")
    write("Analysis performed on cleaned URLs", file = "log.txt", append = TRUE)
  }

  # get a list of all the shared URLs
  URLs <- as.data.frame(table(ct_shares.df$expanded))
  names(URLs) <- c("URL", "ct_shares")
  URLs <- subset(URLs, URLs$ct_shares>1) # remove the URLs shared just 1 time
  URLs$URL <- as.character(URLs$URL)

  # keep only shares of URLs shared more than one time
  ct_shares.df <- subset(ct_shares.df, ct_shares.df$expanded %in% URLs$URL)

  if (nrow(URLs) == 0) {
  stop("### Not enought URLs! ###")
  }

  if (nrow(ct_shares.df) == 0) {
  stop("### Not enought shares! ###")
  }

  ###############
  # Parallel ####
  ###############

  if(parallel==TRUE){

    # setup parallel backend
    cores <- parallel::detectCores()-1
    cl <- parallel::makeCluster(cores, type = "PSOCK")
    doSNOW::registerDoSNOW(cl)

    #rinomino le colonne per farle accettare dalle funzioni di coortweet
    ct_shares.df_new <- prep_data(ct_shares.df,
                                  object_id = "expanded",
                                  account_id = "account.url",
                                  content_id = "id",
                                  timestamp_share = "date")

    #Converto la stringa che indica il coordination interval in un numero
    #leggibile dalla funzione di CooRTweet
    coord_interval_to_numeric <- function(coordination_interval_raw) {
      coord_interval <- as.numeric(sub(" secs", "", coordination_interval_raw))
      return(coord_interval)
    }
    coordination_interval <- coord_interval_to_numeric(coordination_interval)

    #Uso la funzione di coortweet per rilevare le rapid shares
    result <- detect_groups(ct_shares.df_new,
                            min_participation = 2,
                            time_window = coordination_interval)

    parallel::stopCluster(cl)

    #una volta trovate, le vado a etichettare come farebbe CooRnet
    ct_shares.df$is_coordinated <- ifelse(ct_shares.df$expanded %in% result$object_id &
                                            (ct_shares.df$account.url %in% result$account_id | ct_shares.df$account.url %in% result$account_id_y) &
                                            (ct_shares.df$id %in% result$content_id | ct_shares.df$id %in% result$content_id_y), TRUE, FALSE)

    #genero il grafo usando la funzione di CooRTweet
    highly_connected_g <- generate_coordinated_network(result,
                                           edge_weight = percentile_edge_weight,
                                           objects = TRUE)
    #Ricavo le info degli account come farebbe CooRnet
    all_account_info <- ct_shares.df %>%
      dplyr::group_by(account.url) %>%
      dplyr::mutate(account.name.changed = ifelse(length(unique(account.name))>1, TRUE, FALSE), # deal with account.data that may have changed (name, handle, pageAdminTopCountry, pageDescription, pageCategory)
                    account.name = paste(unique(account.name), collapse = " | "),
                    account.handle.changed = ifelse(length(unique(account.handle))>1, TRUE, FALSE),
                    account.handle = paste(unique(account.handle), collapse = " | "),
                    account.pageAdminTopCountry.changed = ifelse(length(unique(account.pageAdminTopCountry))>1, TRUE, FALSE),
                    account.pageAdminTopCountry = paste(unique(account.pageAdminTopCountry), collapse = " | "),
                    account.pageDescription.changed = ifelse(length(unique(account.pageDescription))>1, TRUE, FALSE),
                    account.pageDescription = paste(unique(account.pageDescription), collapse = " | "),
                    account.pageCategory.changed = ifelse(length(unique(account.pageCategory))>1, TRUE, FALSE),
                    account.pageCategory = paste(unique(account.pageCategory), collapse = " | ")) %>%
      dplyr::summarize(shares = n(),
                       coord.shares = sum(is_coordinated==TRUE),
                       account.avg.subscriberCount=mean(account.subscriberCount),
                       account.name = dplyr::first(account.name), # name
                       account.name.changed = dplyr::first(account.name.changed),
                       account.handle.changed = dplyr::first(account.handle.changed), # handle
                       account.handle = dplyr::first(account.handle),
                       account.pageAdminTopCountry.changed= dplyr::first(account.pageAdminTopCountry.changed), # AdminTopCountry
                       account.pageAdminTopCountry= dplyr::first(account.pageAdminTopCountry),
                       account.pageDescription.changed = dplyr::first(account.pageDescription.changed), # PageDescription
                       account.pageDescription = dplyr::first(account.pageDescription),
                       account.pageCategory.changed = dplyr::first(account.pageCategory.changed), # PageCategory
                       account.pageCategory = dplyr::first(account.pageCategory),
                       account.pageCreatedDate = dplyr::first(account.pageCreatedDate), # DateCreated
                       account.platform = dplyr::first(account.platform),
                       account.platformId = dplyr::first(account.platformId),
                       account.verified = dplyr::first(account.verified),
                       account.accountType = dplyr::first(account.accountType))

    # Ricavo il sottoinsieme di account_info, tenendo solo gli account che sono
    # presenti nel grafo generato da CooRTweet, in modo da avere gli attributi
    # pronti da inserire nei vertici
    vertex.info <- subset(all_account_info, as.character(all_account_info$account.url) %in% V(highly_connected_g)$name)

    #Inserisco gli attributi nei vertici del grafo generato da CooRTweet
    V(highly_connected_g)$shares <- sapply(V(highly_connected_g)$name, function(x) vertex.info$shares[vertex.info$account.url == x])
    V(highly_connected_g)$coord.shares <- sapply(V(highly_connected_g)$name, function(x) vertex.info$coord.shares[vertex.info$account.url == x])
    V(highly_connected_g)$account.avg.subscriberCount <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.avg.subscriberCount[vertex.info$account.url == x])
    V(highly_connected_g)$account.platform <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.platform[vertex.info$account.url == x])
    V(highly_connected_g)$account.name <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.name[vertex.info$account.url == x])
    V(highly_connected_g)$account.platformId<- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.platformId[vertex.info$account.url == x])
    V(highly_connected_g)$name.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.name.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.handle <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.handle[vertex.info$account.url == x])
    V(highly_connected_g)$handle.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.handle.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.verified <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.verified[vertex.info$account.url == x])
    V(highly_connected_g)$pageAdminTopCountry.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageAdminTopCountry.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageAdminTopCountry <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageAdminTopCountry[vertex.info$account.url == x])
    V(highly_connected_g)$account.accountType <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.accountType[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageCreatedDate <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageCreatedDate[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageDescription <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageDescription[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageDescription.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageDescription.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageCategory <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageCategory[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageCategory.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageCategory.changed[vertex.info$account.url == x])


    # find and annotate nodes-components
    V(highly_connected_g)$degree <- igraph::degree(highly_connected_g)
    V(highly_connected_g)$component <- igraph::components(highly_connected_g)$membership
    V(highly_connected_g)$cluster <- igraph::cluster_louvain(highly_connected_g)$membership # add cluster to simplyfy the analysis of large components
    V(highly_connected_g)$degree <- igraph::degree(highly_connected_g) # re-calculate the degree on the subgraph
    V(highly_connected_g)$strength <- igraph::strength(highly_connected_g) # sum up the edge weights of the adjacent edges for each vertex

    # Ricavo la tabella dal grafo e numero le righe
    highly_connected_coordinated_entities <- igraph::as_data_frame(highly_connected_g, "vertices")
    rownames(highly_connected_coordinated_entities) <- 1:nrow(highly_connected_coordinated_entities)

    # Ricavo le URLs condivise per il log
    uniqueURLs_shared <- unique(ct_shares.df[, c("expanded", "is_coordinated")])

    #write the log
     write(paste("\nnumber of unique URLs shared in coordinated way:", table(uniqueURLs_shared$is_coordinated)[2][[1]], paste0("(", round((table(uniqueURLs_shared$is_coordinated)[2][[1]]/nrow(uniqueURLs_shared)),4)*100, "%)"),
                 "\nnumber of unique URLs shared in non-coordinated way:", table(uniqueURLs_shared$is_coordinated)[1][[1]], paste0("(", round((table(uniqueURLs_shared$is_coordinated)[1][[1]]/nrow(uniqueURLs_shared)),4)*100, "%)"),
                 "\npercentile_edge_weight:", percentile_edge_weight,
                 "\nhighly connected coordinated entities:", length(unique(highly_connected_coordinated_entities$name)),
                 "\nnumber of component:", length(unique(highly_connected_coordinated_entities$component))),
           file="log.txt", append=TRUE)

    results_list <- list(ct_shares.df, highly_connected_g, highly_connected_coordinated_entities)

    return(results_list)
  }

  ###################
  # Non Parallel ####
  ###################

  if(parallel == FALSE){

    #rinomino le colonne per farle accettare dalle funzioni di coortweet
    ct_shares.df_new <- prep_data(ct_shares.df,
               object_id = "expanded",
               account_id = "account.url",
               content_id = "id",
               timestamp_share = "date")

    #Converto la stringa che indica il coordination interval in un numero
    #leggibile dalla funzione di CooRTweet
    coord_interval_to_numeric <- function(coordination_interval_raw) {
      coord_interval <- as.numeric(sub(" secs", "", coordination_interval_raw))
      return(coord_interval)
    }
    coordination_interval <- coord_interval_to_numeric(coordination_interval)

    #Uso la funzione di coortweet per rilevare le rapid shares
    result <- detect_groups(ct_shares.df_new,
                            min_participation = 2,
                            time_window = coordination_interval)

    # una volta trovate, le vado a etichettare come farebbe coornet
    ct_shares.df$is_coordinated <- ifelse(ct_shares.df$expanded %in% result$object_id &
                                            (ct_shares.df$account.url %in% result$account_id | ct_shares.df$account.url %in% result$account_id_y) &
                                            (ct_shares.df$id %in% result$content_id | ct_shares.df$id %in% result$content_id_y), TRUE, FALSE)

    #genero il grafo usando la funzione di CooRTweet
    highly_connected_g <- generate_coordinated_network(result,
                                            edge_weight = percentile_edge_weight,
                                            objects = TRUE)

    #Ricavo le info degli account come farebbe CooRnet
    all_account_info <- ct_shares.df %>%
      dplyr::group_by(account.url) %>%
      dplyr::mutate(account.name.changed = ifelse(length(unique(account.name))>1, TRUE, FALSE), # deal with account.data that may have changed (name, handle, pageAdminTopCountry, pageDescription, pageCategory)
                    account.name = paste(unique(account.name), collapse = " | "),
                    account.handle.changed = ifelse(length(unique(account.handle))>1, TRUE, FALSE),
                    account.handle = paste(unique(account.handle), collapse = " | "),
                    account.pageAdminTopCountry.changed = ifelse(length(unique(account.pageAdminTopCountry))>1, TRUE, FALSE),
                    account.pageAdminTopCountry = paste(unique(account.pageAdminTopCountry), collapse = " | "),
                    account.pageDescription.changed = ifelse(length(unique(account.pageDescription))>1, TRUE, FALSE),
                    account.pageDescription = paste(unique(account.pageDescription), collapse = " | "),
                    account.pageCategory.changed = ifelse(length(unique(account.pageCategory))>1, TRUE, FALSE),
                    account.pageCategory = paste(unique(account.pageCategory), collapse = " | ")) %>%
      dplyr::summarize(shares = n(),
                       coord.shares = sum(is_coordinated==TRUE),
                       account.avg.subscriberCount=mean(account.subscriberCount),
                       account.name = dplyr::first(account.name), # name
                       account.name.changed = dplyr::first(account.name.changed),
                       account.handle.changed = dplyr::first(account.handle.changed), # handle
                       account.handle = dplyr::first(account.handle),
                       account.pageAdminTopCountry.changed= dplyr::first(account.pageAdminTopCountry.changed), # AdminTopCountry
                       account.pageAdminTopCountry= dplyr::first(account.pageAdminTopCountry),
                       account.pageDescription.changed = dplyr::first(account.pageDescription.changed), # PageDescription
                       account.pageDescription = dplyr::first(account.pageDescription),
                       account.pageCategory.changed = dplyr::first(account.pageCategory.changed), # PageCategory
                       account.pageCategory = dplyr::first(account.pageCategory),
                       account.pageCreatedDate = dplyr::first(account.pageCreatedDate), # DateCreated
                       account.platform = dplyr::first(account.platform),
                       account.platformId = dplyr::first(account.platformId),
                       account.verified = dplyr::first(account.verified),
                       account.accountType = dplyr::first(account.accountType))

    # Ricavo il sottoinsieme di account_info, tenendo solo gli account che sono
    # presenti nel grafo generato da CooRTweet, in modo da avere gli attributi
    # pronti da inserire nei vertici
    vertex.info <- subset(all_account_info, as.character(all_account_info$account.url) %in% V(highly_connected_g)$name)

    #Inserisco gli attributi nei vertici del grafo generato da CooRTweet
    V(highly_connected_g)$shares <- sapply(V(highly_connected_g)$name, function(x) vertex.info$shares[vertex.info$account.url == x])
    V(highly_connected_g)$coord.shares <- sapply(V(highly_connected_g)$name, function(x) vertex.info$coord.shares[vertex.info$account.url == x])
    V(highly_connected_g)$account.avg.subscriberCount <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.avg.subscriberCount[vertex.info$account.url == x])
    V(highly_connected_g)$account.platform <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.platform[vertex.info$account.url == x])
    V(highly_connected_g)$account.name <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.name[vertex.info$account.url == x])
    V(highly_connected_g)$account.platformId<- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.platformId[vertex.info$account.url == x])
    V(highly_connected_g)$name.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.name.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.handle <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.handle[vertex.info$account.url == x])
    V(highly_connected_g)$handle.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.handle.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.verified <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.verified[vertex.info$account.url == x])
    V(highly_connected_g)$pageAdminTopCountry.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageAdminTopCountry.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageAdminTopCountry <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageAdminTopCountry[vertex.info$account.url == x])
    V(highly_connected_g)$account.accountType <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.accountType[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageCreatedDate <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageCreatedDate[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageDescription <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageDescription[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageDescription.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageDescription.changed[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageCategory <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageCategory[vertex.info$account.url == x])
    V(highly_connected_g)$account.pageCategory.changed <- sapply(V(highly_connected_g)$name, function(x) vertex.info$account.pageCategory.changed[vertex.info$account.url == x])



    # find and annotate nodes-components
    V(highly_connected_g)$degree <- igraph::degree(highly_connected_g)
    V(highly_connected_g)$component <- igraph::components(highly_connected_g)$membership
    V(highly_connected_g)$cluster <- igraph::cluster_louvain(highly_connected_g)$membership # add cluster to simplyfy the analysis of large components
    V(highly_connected_g)$degree <- igraph::degree(highly_connected_g) # re-calculate the degree on the subgraph
    V(highly_connected_g)$strength <- igraph::strength(highly_connected_g) # sum up the edge weights of the adjacent edges for each vertex


    # Ricavo la tabella dal grafo e numero le righe
    highly_connected_coordinated_entities <- igraph::as_data_frame(highly_connected_g, "vertices")
    rownames(highly_connected_coordinated_entities) <- 1:nrow(highly_connected_coordinated_entities)

    # Ricavo le URLs condivise per il log
    uniqueURLs_shared <- unique(ct_shares.df[, c("expanded", "is_coordinated")])

    # write the log
    write(paste("\nnumber of unique URLs shared in coordinated way:", table(uniqueURLs_shared$is_coordinated)[2][[1]], paste0("(", round((table(uniqueURLs_shared$is_coordinated)[2][[1]]/nrow(uniqueURLs_shared)),4)*100, "%)"),
                "\nnumber of unique URLs shared in non-coordinated way:", table(uniqueURLs_shared$is_coordinated)[1][[1]], paste0("(", round((table(uniqueURLs_shared$is_coordinated)[1][[1]]/nrow(uniqueURLs_shared)),4)*100, "%)"),
                "\npercentile_edge_weight:", percentile_edge_weight,
                "\nhighly connected coordinated entities:", length(unique(highly_connected_coordinated_entities$name)),
                "\nnumber of component:", length(unique(highly_connected_coordinated_entities$component))),
          file="log.txt", append=TRUE)

    results_list <- list(ct_shares.df, highly_connected_g, highly_connected_coordinated_entities)

    return(results_list)
  }
}
