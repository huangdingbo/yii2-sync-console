<?php


namespace dsj\sync\console\chain;

use dsj\components\helpers\LogHelper;
use dsj\components\server\ParseCrontab;
use yii\helpers\Json;

/**
 * Class IsExecuteHandler
 * @package console\modules\sync\chain
 * 判断是否执行
 */
class IsExecuteHandler extends AbsHandler
{

    public function handlerSync($data_obj,$source_db,$aid_db)
    {
        $res = ParseCrontab::check(time(),$data_obj->sync_rule);

        if (!$res){
            $this->nextHandler = null;
        }

        if (date('Y-m-d H:i',$data_obj->start_timestamp) == date('Y-m-d H:i',time())){
            $this->nextHandler = null;
        }

        if ($this->nextHandler){
            $data_obj->start_timestamp = time();
            $data_obj->status = 1;
            if ($data_obj->extra == 0){
                $extra = [];
                $data_obj->extra = Json::encode($extra);
            }
            $pid = [];
            $data_obj->pid = Json::encode($pid);
            $data_obj->save();
            (new LogHelper())->setFileName('sync.txt')->setRoute(__CLASS__)->setData('任务:<' . $data_obj->name . '>开始执行')->write();
            (new LogHelper())->setFileName($data_obj->id . '.pid')->setRoute(__CLASS__)
                ->setType(FILE_BINARY)
                ->setIsFormat(false)
                ->setData('')
                ->write();
        }

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj,$source_db,$aid_db);
        }
    }
}