<?php


namespace dsj\sync\console\chain;


use dsj\components\helpers\processes\MultipleProcessesHelper;
use yii\helpers\Json;

class ExecuteHandler extends AbsHandler
{

    public function handlerSync($data_obj,$source_db,$aid_db)
    {
        $tables = Json::decode($data_obj->sync_tables,true);

        $tableNum = count($tables);

        $processParams = [];

        if ($data_obj->process_num < $tableNum){
            foreach ($tables as $key => $table){
                if ($key > $data_obj->process_num-1){
                    $index = $key - $data_obj->process_num;
                    while (1){
                        if ($index >= $data_obj->process_num){
                            $index--;
                        }else{
                            break;
                        }
                    }
                    $processParams[$index][] = $table;
                }else{
                    $processParams[$key] = [$table];
                }
            }
        }else{
            $data_obj->process_num = $tableNum;
            foreach ($tables as $key => $table){
                $processParams[$key] = [$table];
            }
        }

        $data_obj->save();

        $params = [
            'processParams' => $processParams,
            'data_obj' => $data_obj
        ];

        $process = new MultipleProcessesHelper();
        $process->setProcessNum($data_obj->process_num);
        $process->setHandler(ProcessHandler::className(),$process,$params);
//        $process->setAfterExecute(function ()use ($data_obj){
//            $data_obj->end_timestamp = time();
//            $data_obj->execute_time = $data_obj->end_timestamp - $data_obj->start_timestamp;
//            $data_obj->status = 3;
//
//            $config = \Yii::$app->getComponents()['db'];
//            unset($config['class']);
//            $db = new Connection($config);
//            $db->open();
//            $db->createCommand("UPDATE t_sync_task SET end_timestamp={$data_obj->end_timestamp}, execute_time={$data_obj->execute_time}, status={$data_obj->status} WHERE id={$data_obj->id}")->execute();
//            $db->close();
//        });
        $process->execute();

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj,$source_db,$aid_db);
        }
    }
}