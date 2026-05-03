package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class TransactionController {

    @Autowired
    private UserRepository userRepository;


    @GetMapping("balance")
    public Balance getBalance(@RequestParam long userId){
        UserRecord user = userRepository.findById(userId);
        if(user != null){
            return new Balance((float) user.getBalance());
        }
        return  new Balance(0f);
    }
}
