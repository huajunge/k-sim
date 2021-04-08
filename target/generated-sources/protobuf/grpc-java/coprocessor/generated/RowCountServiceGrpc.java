package coprocessor.generated;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.6.1)",
    comments = "Source: RowCountService.proto")
public final class RowCountServiceGrpc {

  private RowCountServiceGrpc() {}

  public static final String SERVICE_NAME = "RowCountService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<coprocessor.generated.RowCounterProtos.CountRequest,
      coprocessor.generated.RowCounterProtos.CountResponse> METHOD_GET_ROW_COUNT =
      io.grpc.MethodDescriptor.<coprocessor.generated.RowCounterProtos.CountRequest, coprocessor.generated.RowCounterProtos.CountResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "RowCountService", "getRowCount"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              coprocessor.generated.RowCounterProtos.CountRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              coprocessor.generated.RowCounterProtos.CountResponse.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<coprocessor.generated.RowCounterProtos.CountRequest,
      coprocessor.generated.RowCounterProtos.CountResponse> METHOD_GET_CELL_COUNT =
      io.grpc.MethodDescriptor.<coprocessor.generated.RowCounterProtos.CountRequest, coprocessor.generated.RowCounterProtos.CountResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "RowCountService", "getCellCount"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              coprocessor.generated.RowCounterProtos.CountRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              coprocessor.generated.RowCounterProtos.CountResponse.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RowCountServiceStub newStub(io.grpc.Channel channel) {
    return new RowCountServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RowCountServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RowCountServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RowCountServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RowCountServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class RowCountServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getRowCount(coprocessor.generated.RowCounterProtos.CountRequest request,
        io.grpc.stub.StreamObserver<coprocessor.generated.RowCounterProtos.CountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_ROW_COUNT, responseObserver);
    }

    /**
     */
    public void getCellCount(coprocessor.generated.RowCounterProtos.CountRequest request,
        io.grpc.stub.StreamObserver<coprocessor.generated.RowCounterProtos.CountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CELL_COUNT, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_ROW_COUNT,
            asyncUnaryCall(
              new MethodHandlers<
                coprocessor.generated.RowCounterProtos.CountRequest,
                coprocessor.generated.RowCounterProtos.CountResponse>(
                  this, METHODID_GET_ROW_COUNT)))
          .addMethod(
            METHOD_GET_CELL_COUNT,
            asyncUnaryCall(
              new MethodHandlers<
                coprocessor.generated.RowCounterProtos.CountRequest,
                coprocessor.generated.RowCounterProtos.CountResponse>(
                  this, METHODID_GET_CELL_COUNT)))
          .build();
    }
  }

  /**
   */
  public static final class RowCountServiceStub extends io.grpc.stub.AbstractStub<RowCountServiceStub> {
    private RowCountServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RowCountServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RowCountServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RowCountServiceStub(channel, callOptions);
    }

    /**
     */
    public void getRowCount(coprocessor.generated.RowCounterProtos.CountRequest request,
        io.grpc.stub.StreamObserver<coprocessor.generated.RowCounterProtos.CountResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_ROW_COUNT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCellCount(coprocessor.generated.RowCounterProtos.CountRequest request,
        io.grpc.stub.StreamObserver<coprocessor.generated.RowCounterProtos.CountResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CELL_COUNT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RowCountServiceBlockingStub extends io.grpc.stub.AbstractStub<RowCountServiceBlockingStub> {
    private RowCountServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RowCountServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RowCountServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RowCountServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public coprocessor.generated.RowCounterProtos.CountResponse getRowCount(coprocessor.generated.RowCounterProtos.CountRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_ROW_COUNT, getCallOptions(), request);
    }

    /**
     */
    public coprocessor.generated.RowCounterProtos.CountResponse getCellCount(coprocessor.generated.RowCounterProtos.CountRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CELL_COUNT, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RowCountServiceFutureStub extends io.grpc.stub.AbstractStub<RowCountServiceFutureStub> {
    private RowCountServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RowCountServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RowCountServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RowCountServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<coprocessor.generated.RowCounterProtos.CountResponse> getRowCount(
        coprocessor.generated.RowCounterProtos.CountRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_ROW_COUNT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<coprocessor.generated.RowCounterProtos.CountResponse> getCellCount(
        coprocessor.generated.RowCounterProtos.CountRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CELL_COUNT, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_ROW_COUNT = 0;
  private static final int METHODID_GET_CELL_COUNT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RowCountServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RowCountServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_ROW_COUNT:
          serviceImpl.getRowCount((coprocessor.generated.RowCounterProtos.CountRequest) request,
              (io.grpc.stub.StreamObserver<coprocessor.generated.RowCounterProtos.CountResponse>) responseObserver);
          break;
        case METHODID_GET_CELL_COUNT:
          serviceImpl.getCellCount((coprocessor.generated.RowCounterProtos.CountRequest) request,
              (io.grpc.stub.StreamObserver<coprocessor.generated.RowCounterProtos.CountResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class RowCountServiceDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return coprocessor.generated.RowCounterProtos.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RowCountServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RowCountServiceDescriptorSupplier())
              .addMethod(METHOD_GET_ROW_COUNT)
              .addMethod(METHOD_GET_CELL_COUNT)
              .build();
        }
      }
    }
    return result;
  }
}
