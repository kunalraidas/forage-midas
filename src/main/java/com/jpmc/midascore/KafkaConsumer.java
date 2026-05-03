package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;
    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    private RestTemplate restTemplate = new RestTemplate();

    @Transactional
    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )

    public void  listen(Transaction transaction){
        UserRecord senderId = userRepository.findById(transaction.getSenderId());
        UserRecord recipientId   = userRepository.findById(transaction.getRecipientId());
        if(senderId != null && recipientId != null && senderId.getBalance()>= transaction.getAmount()){

            Incentive incentive = restTemplate.postForObject(
                    "http://localhost:8080/incentive",
                    transaction,
                    Incentive.class
            );

            // update
            senderId.setBalance(senderId.getBalance() - transaction.getAmount());
            recipientId.setBalance(recipientId.getBalance() + transaction.getAmount());

            if (incentive != null) {
                recipientId.setBalance((float) (recipientId.getBalance() + incentive.getAmount()));
            }
            // save
            userRepository.save(senderId);
            userRepository.save(recipientId);

            TransactionRecord newRecord = new TransactionRecord(senderId,recipientId, transaction.getAmount());
            transactionRecordRepository.save(newRecord);
        }
        System.out.println("SENDER: " + senderId.getName() + " | Balance: " + senderId.getBalance());
        System.out.println("RECIPIENT: " + recipientId.getName() + " | Balance: " + recipientId.getBalance());
        System.out.println("---");
    }

}
