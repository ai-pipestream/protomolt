package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;

/**
 * An indexed message with the two facts about it the diff rules need but the descriptor itself
 * does not carry at hand: the syntax of the file that declared it, and whether it is a synthetic
 * map entry rather than a message an author wrote.
 */
record MessageInfo(DescriptorProto proto, boolean proto3, boolean mapEntry) {
}
