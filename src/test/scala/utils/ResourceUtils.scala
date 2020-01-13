package utils

trait ResourceUtils {

  /**
    * Get a File object of a given resource path
    * @param path relative path
    * @return full path
    */
  def getResourceFullPath(path: String): String =
    getClass.getResource(ensureLeadingSlash(path)).toURI.getPath

  /**
    * Add a leading slash if necessary
    * @param path path
    * @return path if leading slash
    */
  private def ensureLeadingSlash(path: String): String =
    if (path.startsWith("/")) path else s"/$path"

}
