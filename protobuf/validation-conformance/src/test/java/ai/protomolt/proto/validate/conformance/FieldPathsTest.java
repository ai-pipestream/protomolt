package ai.protomolt.proto.validate.conformance;

import ai.protomolt.proto.validate.conformance.testdata.v1.Cases;
import build.buf.validate.FieldPath;
import build.buf.validate.FieldPathElement;
import build.buf.validate.FieldPathElement.SubscriptCase;
import build.buf.validate.FieldRules;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the field-path port produces the same structured {@link FieldPath} the conformance
 * runner reconstructs from its expected dotted strings. These are the paths compared with
 * {@code proto.Equal}, so structural correctness here is what earns conformance matches.
 */
class FieldPathsTest {

    private static final Descriptor PERSON = Cases.Person.getDescriptor();
    private static final Descriptor SIGNUP = Cases.Signup.getDescriptor();
    private static final Descriptor FIELD_RULES = FieldRules.getDescriptor();

    @Test
    void scalarField() {
        FieldPath path = FieldPaths.unmarshal(PERSON, "age");
        assertThat(path.getElementsCount()).isEqualTo(1);
        FieldPathElement e = path.getElements(0);
        assertThat(e.getFieldName()).isEqualTo("age");
        assertThat(e.getFieldNumber()).isEqualTo(PERSON.findFieldByName("age").getNumber());
        assertThat(e.getFieldType()).isEqualTo(Type.TYPE_INT32);
        assertThat(e.getSubscriptCase()).isEqualTo(SubscriptCase.SUBSCRIPT_NOT_SET);
    }

    @Test
    void repeatedElementIndex() {
        FieldPath path = FieldPaths.unmarshal(PERSON, "codes[2]");
        assertThat(path.getElementsCount()).isEqualTo(1);
        FieldPathElement e = path.getElements(0);
        assertThat(e.getFieldName()).isEqualTo("codes");
        assertThat(e.getFieldType()).isEqualTo(Type.TYPE_STRING);
        assertThat(e.getSubscriptCase()).isEqualTo(SubscriptCase.INDEX);
        assertThat(e.getIndex()).isEqualTo(2);
    }

    @Test
    void mapStringKey() {
        FieldPath path = FieldPaths.unmarshal(PERSON, "scores[\"k\"]");
        assertThat(path.getElementsCount()).isEqualTo(1);
        FieldPathElement e = path.getElements(0);
        assertThat(e.getFieldName()).isEqualTo("scores");
        assertThat(e.getFieldType()).isEqualTo(Type.TYPE_MESSAGE);
        assertThat(e.getKeyType()).isEqualTo(Type.TYPE_STRING);
        assertThat(e.getValueType()).isEqualTo(Type.TYPE_INT32);
        assertThat(e.getSubscriptCase()).isEqualTo(SubscriptCase.STRING_KEY);
        assertThat(e.getStringKey()).isEqualTo("k");
    }

    @Test
    void mapIntegerKey() {
        FieldPathElement e = FieldPaths.unmarshal(PERSON, "ledger[42]").getElements(0);
        assertThat(e.getKeyType()).isEqualTo(Type.TYPE_INT64);
        assertThat(e.getSubscriptCase()).isEqualTo(SubscriptCase.INT_KEY);
        assertThat(e.getIntKey()).isEqualTo(42);
    }

    @Test
    void mapUnsignedKey() {
        FieldPathElement e = FieldPaths.unmarshal(PERSON, "counts[7]").getElements(0);
        assertThat(e.getKeyType()).isEqualTo(Type.TYPE_UINT32);
        assertThat(e.getSubscriptCase()).isEqualTo(SubscriptCase.UINT_KEY);
        assertThat(e.getUintKey()).isEqualTo(7);
    }

    @Test
    void mapBoolKey() {
        FieldPathElement e = FieldPaths.unmarshal(PERSON, "flags[true]").getElements(0);
        assertThat(e.getKeyType()).isEqualTo(Type.TYPE_BOOL);
        assertThat(e.getSubscriptCase()).isEqualTo(SubscriptCase.BOOL_KEY);
        assertThat(e.getBoolKey()).isTrue();
    }

    @Test
    void nestedMessageField() {
        FieldPath path = FieldPaths.unmarshal(SIGNUP, "person.age");
        assertThat(path.getElementsCount()).isEqualTo(2);
        assertThat(path.getElements(0).getFieldName()).isEqualTo("person");
        assertThat(path.getElements(0).getFieldType()).isEqualTo(Type.TYPE_MESSAGE);
        assertThat(path.getElements(1).getFieldName()).isEqualTo("age");
        assertThat(path.getElements(1).getFieldType()).isEqualTo(Type.TYPE_INT32);
    }

    @Test
    void rulePathTwoLevels() {
        FieldPath path = FieldPaths.unmarshal(FIELD_RULES, "int32.gte");
        assertThat(path.getElementsCount()).isEqualTo(2);
        assertThat(path.getElements(0).getFieldName()).isEqualTo("int32");
        assertThat(path.getElements(0).getFieldNumber())
                .isEqualTo(FIELD_RULES.findFieldByName("int32").getNumber());
        assertThat(path.getElements(1).getFieldName()).isEqualTo("gte");
    }

    @Test
    void rulePathDescendsThroughRepeatedItems() {
        FieldPath path = FieldPaths.unmarshal(FIELD_RULES, "repeated.items.string.min_len");
        assertThat(path.getElementsCount()).isEqualTo(4);
        assertThat(path.getElements(0).getFieldName()).isEqualTo("repeated");
        assertThat(path.getElements(1).getFieldName()).isEqualTo("items");
        assertThat(path.getElements(2).getFieldName()).isEqualTo("string");
        assertThat(path.getElements(3).getFieldName()).isEqualTo("min_len");
    }

    @Test
    void rulePathScalar() {
        FieldPath path = FieldPaths.unmarshal(FIELD_RULES, "required");
        assertThat(path.getElementsCount()).isEqualTo(1);
        assertThat(path.getElements(0).getFieldName()).isEqualTo("required");
    }
}
