package hank_util

import (
  "path"
  "path/filepath"
)

func IsSubdirectory(root string, otherPath string) bool {

  cleanRoot := path.Clean(root)
  cleanRel := path.Clean(otherPath)

  if cleanRoot == cleanRel {
    return false
  }

  rel, _ := filepath.Rel(root, otherPath);
  return path.Join(cleanRoot, rel) == cleanRel

}
