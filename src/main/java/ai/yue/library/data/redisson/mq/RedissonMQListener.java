package ai.yue.library.data.redisson.mq;

import ai.yue.library.data.redisson.annotation.MQListener;
import org.redisson.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * mq监听
 */
public class RedissonMQListener implements BeanPostProcessor, Closeable {


    Logger log = LoggerFactory.getLogger(RedissonMQListener.class);

    @Autowired
    private RedissonClient redissonClient;

    private ExecutorService consumerExecutor;

    private AtomicBoolean CONSUMER_ACTIVE = new AtomicBoolean(true);

    private final int READ_TIMEOUT = 1000;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            MQListener annotation = AnnotationUtils.findAnnotation(method, MQListener.class);
            if(annotation!=null){

                if(consumerExecutor == null){
                    consumerExecutor = Executors.newCachedThreadPool();
                }

                String topic = annotation.name();
                int concurrent = annotation.concurrent();
                concurrent = concurrent <= 1 ? 1 : concurrent;
                RStream<String,String> rStream = redissonClient.getStream(topic);
                log.info("构建消费者name={}",annotation.name());

                for (int threadId = 0; threadId < concurrent; threadId++) {
                    final String consumerId = topic + "_consumer" + threadId;
                    log.info("启动消费者线程 {}", consumerId);
                    consumerExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            //消费消息线程
                            while (CONSUMER_ACTIVE.get()) {
                                Map<StreamMessageId, Map<String, String>> msgHolder = rStream.readGroup(topic, consumerId, 1, READ_TIMEOUT, TimeUnit.MILLISECONDS);

                                if (msgHolder != null && msgHolder.size() > 0) {
                                    for (Map.Entry<StreamMessageId, Map<String, String>> entry : msgHolder.entrySet()) {
                                        Map<String, String> msg = entry.getValue();
                                        log.info("线程ID {},线程任务名 {},消费者ID {},消息ID {},消息体 {}", Thread.currentThread().getId(), Thread.currentThread().getName(), consumerId, entry.getKey(), msg);
                                        //执行消息处理
                                        boolean success = true;
                                        try {
                                            Object[] aras=new Object[method.getParameterTypes().length];
                                            int index=0;
                                            for (Class parameterType : method.getParameterTypes()) {
                                                String simpleName = parameterType.getSimpleName();
                                                if (msg.getClass().getSimpleName().equals(simpleName)||"Object".equals(simpleName)){
                                                    aras[index++]=msg;
                                                }else {
                                                    aras[index++]=null;
                                                }
                                            }
                                            method.invoke(bean,aras);
                                        } catch (Exception e) {
                                            success = false;
                                            throw new RuntimeException(e);
                                        }finally {
                                            if (success) {
                                                //消费了消息，自动应答ACK
                                                rStream.ack(topic, entry.getKey());
                                            }
                                        }
                                    }
                                } else {
                                    //log.info("无消息可消费");
                                }
                            }
                        }
                    });
                }

                /*
                topic.addListener(Object.class, (channel, msg) -> {
                    try {
                        Object[] aras=new Object[method.getParameterTypes().length];
                        int index=0;
                        for (Class parameterType : method.getParameterTypes()) {
                            String simpleName = parameterType.getSimpleName();
                            if("CharSequence".equals(simpleName)){
                                aras[index++]=channel;
                            }else if (msg.getClass().getSimpleName().equals(simpleName)||"Object".equals(simpleName)){
                                aras[index++]=msg;
                            }else {
                                aras[index++]=null;
                            }
                        }
                        method.invoke(bean,aras);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                */
            }
        }, ReflectionUtils.USER_DECLARED_METHODS);
        return bean;
    }

    @Override
    public void close() throws IOException {
        CONSUMER_ACTIVE.set(false);
        if (consumerExecutor != null) {
            consumerExecutor.shutdown();
        }
    }
}
