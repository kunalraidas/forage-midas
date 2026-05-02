package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;
    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @Transactional
    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )

    public void  listen(Transaction transaction){
        UserRecord senderId = userRepository.findById(transaction.getSenderId());
        UserRecord recipientId   = userRepository.findById(transaction.getRecipientId());
        if(senderId != null && recipientId != null && senderId.getBalance()>= transaction.getAmount()){

            // update
            senderId.setBalance(senderId.getBalance() - transaction.getAmount());
            recipientId.setBalance(recipientId.getBalance() + transaction.getAmount());

            // save
            userRepository.save(senderId);
            userRepository.save(recipientId);

            TransactionRecord newRecord = new TransactionRecord(senderId,recipientId, transaction.getAmount());
            transactionRecordRepository.save(newRecord);

            //
            System.out.println("User: " + senderId.getName() + " | Balance after: " + (senderId.getBalance() - transaction.getAmount()));
            System.out.println("RECIPIENT: " + recipientId.getName() + " | Balance: " + recipientId.getBalance());
            System.out.println("---");

        }

    }
}
