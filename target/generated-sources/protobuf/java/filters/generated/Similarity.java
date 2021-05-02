// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Similartity.proto

package filters.generated;

public final class Similarity {
  private Similarity() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface SimilarityFilterOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string traj = 1;
    /**
     * <code>required string traj = 1;</code>
     */
    boolean hasTraj();
    /**
     * <code>required string traj = 1;</code>
     */
    java.lang.String getTraj();
    /**
     * <code>required string traj = 1;</code>
     */
    com.google.protobuf.ByteString
        getTrajBytes();

    // optional double threshold = 2;
    /**
     * <code>optional double threshold = 2;</code>
     */
    boolean hasThreshold();
    /**
     * <code>optional double threshold = 2;</code>
     */
    double getThreshold();

    // optional int32 func = 3;
    /**
     * <code>optional int32 func = 3;</code>
     */
    boolean hasFunc();
    /**
     * <code>optional int32 func = 3;</code>
     */
    int getFunc();

    // optional bool returnSim = 4;
    /**
     * <code>optional bool returnSim = 4;</code>
     */
    boolean hasReturnSim();
    /**
     * <code>optional bool returnSim = 4;</code>
     */
    boolean getReturnSim();
  }
  /**
   * Protobuf type {@code SimilarityFilter}
   */
  public static final class SimilarityFilter extends
      com.google.protobuf.GeneratedMessage
      implements SimilarityFilterOrBuilder {
    // Use SimilarityFilter.newBuilder() to construct.
    private SimilarityFilter(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SimilarityFilter(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SimilarityFilter defaultInstance;
    public static SimilarityFilter getDefaultInstance() {
      return defaultInstance;
    }

    public SimilarityFilter getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SimilarityFilter(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              traj_ = input.readBytes();
              break;
            }
            case 17: {
              bitField0_ |= 0x00000002;
              threshold_ = input.readDouble();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              func_ = input.readInt32();
              break;
            }
            case 32: {
              bitField0_ |= 0x00000008;
              returnSim_ = input.readBool();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return filters.generated.Similarity.internal_static_SimilarityFilter_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return filters.generated.Similarity.internal_static_SimilarityFilter_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              filters.generated.Similarity.SimilarityFilter.class, filters.generated.Similarity.SimilarityFilter.Builder.class);
    }

    public static com.google.protobuf.Parser<SimilarityFilter> PARSER =
        new com.google.protobuf.AbstractParser<SimilarityFilter>() {
      public SimilarityFilter parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SimilarityFilter(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SimilarityFilter> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string traj = 1;
    public static final int TRAJ_FIELD_NUMBER = 1;
    private java.lang.Object traj_;
    /**
     * <code>required string traj = 1;</code>
     */
    public boolean hasTraj() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string traj = 1;</code>
     */
    public java.lang.String getTraj() {
      java.lang.Object ref = traj_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          traj_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string traj = 1;</code>
     */
    public com.google.protobuf.ByteString
        getTrajBytes() {
      java.lang.Object ref = traj_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        traj_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional double threshold = 2;
    public static final int THRESHOLD_FIELD_NUMBER = 2;
    private double threshold_;
    /**
     * <code>optional double threshold = 2;</code>
     */
    public boolean hasThreshold() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional double threshold = 2;</code>
     */
    public double getThreshold() {
      return threshold_;
    }

    // optional int32 func = 3;
    public static final int FUNC_FIELD_NUMBER = 3;
    private int func_;
    /**
     * <code>optional int32 func = 3;</code>
     */
    public boolean hasFunc() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int32 func = 3;</code>
     */
    public int getFunc() {
      return func_;
    }

    // optional bool returnSim = 4;
    public static final int RETURNSIM_FIELD_NUMBER = 4;
    private boolean returnSim_;
    /**
     * <code>optional bool returnSim = 4;</code>
     */
    public boolean hasReturnSim() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bool returnSim = 4;</code>
     */
    public boolean getReturnSim() {
      return returnSim_;
    }

    private void initFields() {
      traj_ = "";
      threshold_ = 0D;
      func_ = 0;
      returnSim_ = false;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasTraj()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getTrajBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeDouble(2, threshold_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt32(3, func_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBool(4, returnSim_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getTrajBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeDoubleSize(2, threshold_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, func_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(4, returnSim_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof filters.generated.Similarity.SimilarityFilter)) {
        return super.equals(obj);
      }
      filters.generated.Similarity.SimilarityFilter other = (filters.generated.Similarity.SimilarityFilter) obj;

      boolean result = true;
      result = result && (hasTraj() == other.hasTraj());
      if (hasTraj()) {
        result = result && getTraj()
            .equals(other.getTraj());
      }
      result = result && (hasThreshold() == other.hasThreshold());
      if (hasThreshold()) {
        result = result && (Double.doubleToLongBits(getThreshold())    == Double.doubleToLongBits(other.getThreshold()));
      }
      result = result && (hasFunc() == other.hasFunc());
      if (hasFunc()) {
        result = result && (getFunc()
            == other.getFunc());
      }
      result = result && (hasReturnSim() == other.hasReturnSim());
      if (hasReturnSim()) {
        result = result && (getReturnSim()
            == other.getReturnSim());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasTraj()) {
        hash = (37 * hash) + TRAJ_FIELD_NUMBER;
        hash = (53 * hash) + getTraj().hashCode();
      }
      if (hasThreshold()) {
        hash = (37 * hash) + THRESHOLD_FIELD_NUMBER;
        hash = (53 * hash) + hashLong(
            Double.doubleToLongBits(getThreshold()));
      }
      if (hasFunc()) {
        hash = (37 * hash) + FUNC_FIELD_NUMBER;
        hash = (53 * hash) + getFunc();
      }
      if (hasReturnSim()) {
        hash = (37 * hash) + RETURNSIM_FIELD_NUMBER;
        hash = (53 * hash) + hashBoolean(getReturnSim());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static filters.generated.Similarity.SimilarityFilter parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static filters.generated.Similarity.SimilarityFilter parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static filters.generated.Similarity.SimilarityFilter parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static filters.generated.Similarity.SimilarityFilter parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(filters.generated.Similarity.SimilarityFilter prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code SimilarityFilter}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements filters.generated.Similarity.SimilarityFilterOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return filters.generated.Similarity.internal_static_SimilarityFilter_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return filters.generated.Similarity.internal_static_SimilarityFilter_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                filters.generated.Similarity.SimilarityFilter.class, filters.generated.Similarity.SimilarityFilter.Builder.class);
      }

      // Construct using filters.generated.Similarity.SimilarityFilter.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        traj_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        threshold_ = 0D;
        bitField0_ = (bitField0_ & ~0x00000002);
        func_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        returnSim_ = false;
        bitField0_ = (bitField0_ & ~0x00000008);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return filters.generated.Similarity.internal_static_SimilarityFilter_descriptor;
      }

      public filters.generated.Similarity.SimilarityFilter getDefaultInstanceForType() {
        return filters.generated.Similarity.SimilarityFilter.getDefaultInstance();
      }

      public filters.generated.Similarity.SimilarityFilter build() {
        filters.generated.Similarity.SimilarityFilter result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public filters.generated.Similarity.SimilarityFilter buildPartial() {
        filters.generated.Similarity.SimilarityFilter result = new filters.generated.Similarity.SimilarityFilter(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.traj_ = traj_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.threshold_ = threshold_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.func_ = func_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.returnSim_ = returnSim_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof filters.generated.Similarity.SimilarityFilter) {
          return mergeFrom((filters.generated.Similarity.SimilarityFilter)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(filters.generated.Similarity.SimilarityFilter other) {
        if (other == filters.generated.Similarity.SimilarityFilter.getDefaultInstance()) return this;
        if (other.hasTraj()) {
          bitField0_ |= 0x00000001;
          traj_ = other.traj_;
          onChanged();
        }
        if (other.hasThreshold()) {
          setThreshold(other.getThreshold());
        }
        if (other.hasFunc()) {
          setFunc(other.getFunc());
        }
        if (other.hasReturnSim()) {
          setReturnSim(other.getReturnSim());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasTraj()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        filters.generated.Similarity.SimilarityFilter parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (filters.generated.Similarity.SimilarityFilter) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string traj = 1;
      private java.lang.Object traj_ = "";
      /**
       * <code>required string traj = 1;</code>
       */
      public boolean hasTraj() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string traj = 1;</code>
       */
      public java.lang.String getTraj() {
        java.lang.Object ref = traj_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          traj_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string traj = 1;</code>
       */
      public com.google.protobuf.ByteString
          getTrajBytes() {
        java.lang.Object ref = traj_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          traj_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string traj = 1;</code>
       */
      public Builder setTraj(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        traj_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string traj = 1;</code>
       */
      public Builder clearTraj() {
        bitField0_ = (bitField0_ & ~0x00000001);
        traj_ = getDefaultInstance().getTraj();
        onChanged();
        return this;
      }
      /**
       * <code>required string traj = 1;</code>
       */
      public Builder setTrajBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        traj_ = value;
        onChanged();
        return this;
      }

      // optional double threshold = 2;
      private double threshold_ ;
      /**
       * <code>optional double threshold = 2;</code>
       */
      public boolean hasThreshold() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional double threshold = 2;</code>
       */
      public double getThreshold() {
        return threshold_;
      }
      /**
       * <code>optional double threshold = 2;</code>
       */
      public Builder setThreshold(double value) {
        bitField0_ |= 0x00000002;
        threshold_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional double threshold = 2;</code>
       */
      public Builder clearThreshold() {
        bitField0_ = (bitField0_ & ~0x00000002);
        threshold_ = 0D;
        onChanged();
        return this;
      }

      // optional int32 func = 3;
      private int func_ ;
      /**
       * <code>optional int32 func = 3;</code>
       */
      public boolean hasFunc() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int32 func = 3;</code>
       */
      public int getFunc() {
        return func_;
      }
      /**
       * <code>optional int32 func = 3;</code>
       */
      public Builder setFunc(int value) {
        bitField0_ |= 0x00000004;
        func_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 func = 3;</code>
       */
      public Builder clearFunc() {
        bitField0_ = (bitField0_ & ~0x00000004);
        func_ = 0;
        onChanged();
        return this;
      }

      // optional bool returnSim = 4;
      private boolean returnSim_ ;
      /**
       * <code>optional bool returnSim = 4;</code>
       */
      public boolean hasReturnSim() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional bool returnSim = 4;</code>
       */
      public boolean getReturnSim() {
        return returnSim_;
      }
      /**
       * <code>optional bool returnSim = 4;</code>
       */
      public Builder setReturnSim(boolean value) {
        bitField0_ |= 0x00000008;
        returnSim_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool returnSim = 4;</code>
       */
      public Builder clearReturnSim() {
        bitField0_ = (bitField0_ & ~0x00000008);
        returnSim_ = false;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:SimilarityFilter)
    }

    static {
      defaultInstance = new SimilarityFilter(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:SimilarityFilter)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_SimilarityFilter_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_SimilarityFilter_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\021Similartity.proto\"T\n\020SimilarityFilter\022" +
      "\014\n\004traj\030\001 \002(\t\022\021\n\tthreshold\030\002 \001(\001\022\014\n\004func" +
      "\030\003 \001(\005\022\021\n\treturnSim\030\004 \001(\010B\'\n\021filters.gen" +
      "eratedB\nSimilarityH\001\210\001\001\240\001\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_SimilarityFilter_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_SimilarityFilter_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_SimilarityFilter_descriptor,
              new java.lang.String[] { "Traj", "Threshold", "Func", "ReturnSim", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
