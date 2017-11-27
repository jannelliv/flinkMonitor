package ch.eth.inf.infsec

package object slicer {
  // TODO(JS): Type of data domain?
  // TODO(JS): Consider using a more specialized container type, e.g. Array.
  // TODO(JS): Move to different package, this type is not specific to slicing.
  type Tuple = IndexedSeq[Any]
}
