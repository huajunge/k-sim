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
    comments = "Source: knnServer.proto")
public final class KnnServiceGrpc {

  private KnnServiceGrpc() {}

  public static final String SERVICE_NAME = "KnnService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<coprocessor.generated.KNNServer.KnnRequest,
      coprocessor.generated.KNNServer.KnnResponse> METHOD_GET_TOP_K =
      io.grpc.MethodDescriptor.<coprocessor.generated.KNNServer.KnnRequest, coprocessor.generated.KNNServer.KnnResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "KnnService", "getTopK"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              coprocessor.generated.KNNServer.KnnRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              coprocessor.generated.KNNServer.KnnResponse.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static KnnServiceStub newStub(io.grpc.Channel channel) {
    return new KnnServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static KnnServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new KnnServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static KnnServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new KnnServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class KnnServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getTopK(coprocessor.generated.KNNServer.KnnRequest request,
        io.grpc.stub.StreamObserver<coprocessor.generated.KNNServer.KnnResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_TOP_K, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_TOP_K,
            asyncUnaryCall(
              new MethodHandlers<
                coprocessor.generated.KNNServer.KnnRequest,
                coprocessor.generated.KNNServer.KnnResponse>(
                  this, METHODID_GET_TOP_K)))
          .build();
    }
  }

  /**
   */
  public static final class KnnServiceStub extends io.grpc.stub.AbstractStub<KnnServiceStub> {
    private KnnServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KnnServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KnnServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KnnServiceStub(channel, callOptions);
    }

    /**
     */
    public void getTopK(coprocessor.generated.KNNServer.KnnRequest request,
        io.grpc.stub.StreamObserver<coprocessor.generated.KNNServer.KnnResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_TOP_K, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class KnnServiceBlockingStub extends io.grpc.stub.AbstractStub<KnnServiceBlockingStub> {
    private KnnServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KnnServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KnnServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KnnServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public coprocessor.generated.KNNServer.KnnResponse getTopK(coprocessor.generated.KNNServer.KnnRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_TOP_K, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class KnnServiceFutureStub extends io.grpc.stub.AbstractStub<KnnServiceFutureStub> {
    private KnnServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KnnServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KnnServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KnnServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<coprocessor.generated.KNNServer.KnnResponse> getTopK(
        coprocessor.generated.KNNServer.KnnRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_TOP_K, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_TOP_K = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final KnnServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(KnnServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_TOP_K:
          serviceImpl.getTopK((coprocessor.generated.KNNServer.KnnRequest) request,
              (io.grpc.stub.StreamObserver<coprocessor.generated.KNNServer.KnnResponse>) responseObserver);
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

  private static final class KnnServiceDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return coprocessor.generated.KNNServer.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (KnnServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new KnnServiceDescriptorSupplier())
              .addMethod(METHOD_GET_TOP_K)
              .build();
        }
      }
    }
    return result;
  }
}
