package io.aeron.cluster;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;

public class ClusterInstrumentor
{
    public ClusterInstrumentor(
        final Class<?> adviceClass,
        final String simpleClassName,
        final String methodName)
    {
        final Instrumentation instrumentation = ByteBuddyAgent.install();
        System.out.println(instrumentation);

        final AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);

        agentBuilder
            .type(ElementMatchers.nameEndsWith(simpleClassName))
            .transform(
                (builder, typeDescription, classLoader, module) ->
                    builder.visit(Advice.to(adviceClass).on(ElementMatchers.named(methodName))))
            .installOn(instrumentation);
    }

    static class AgentBuilderListener implements AgentBuilder.Listener
    {
        public void onDiscovery(
            final String typeName, final ClassLoader classLoader, final JavaModule module, final boolean loaded)
        {
        }

        public void onTransformation(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final DynamicType dynamicType)
        {
        }

        public void onIgnored(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
        }

        public void onError(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final Throwable throwable)
        {
            System.err.println(
                "typeName = " + typeName + ", classLoader = " + classLoader + ", module = " + module + ", loaded = " +
                loaded);
            throwable.printStackTrace(System.err);
        }

        public void onComplete(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
        }
    }
}
