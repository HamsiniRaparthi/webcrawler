package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;

/**
 * A concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public <T> T wrap(Class<T> klass, T delegate) {
    Objects.requireNonNull(klass);
    Objects.requireNonNull(delegate);

    if (!klass.isInterface()) {
      throw new IllegalArgumentException(klass.getName() + " is not an interface");
    }

    boolean hasProfiledMethod = Arrays.stream(klass.getMethods())
            .anyMatch(method -> method.isAnnotationPresent(Profiled.class));
    if (!hasProfiledMethod) {
        throw new IllegalArgumentException(
                "The interface " + klass.getName() + " has no methods annotated with @Profiled.");
    }

    ProfilingMethodInterceptor handler = new ProfilingMethodInterceptor(clock, delegate, state);

    Object proxy = Proxy.newProxyInstance(
            klass.getClassLoader(),
            new Class<?>[]{klass},
            handler);

    return (T) proxy;
  }

  @Override
  public void writeData(Path path) throws IOException {
    Objects.requireNonNull(path);
    try (Writer writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      state.write(writer);
    }
  }

  @Override
  public void writeData(Writer writer) throws IOException {
    state.write(writer);
  }
}
