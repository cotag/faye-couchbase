function(doc) {
  if(doc.type == "faye_server_node" && doc.cluster_id != null) {
    emit(doc.cluster_id, null);
  }
}
