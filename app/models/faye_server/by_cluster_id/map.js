function(doc) {
  if(doc.type == "faye_server" && doc.cluster_id != null) {
    emit(doc.cluster_id, null);
  }
}
