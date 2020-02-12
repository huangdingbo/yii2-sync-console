<?php

namespace dsj\sync\console\controllers;

use dsj\sync\console\chain\CheckAidTablesHandler;
use dsj\sync\console\chain\CheckSourceHandler;
use dsj\sync\console\chain\CloseDbHandler;
use dsj\sync\console\chain\EqualTablesHandler;
use dsj\sync\console\chain\ExecuteHandler;
use dsj\sync\console\chain\IsExecuteHandler;
use dsj\sync\console\chain\OpenDbHandler;
use dsj\sync\web\models\TSyncTask;
use Yii;
use yii\console\Controller;
use yii\console\ExitCode;


/**
 * Default controller for the `sync` module
 */
class RunController extends Controller
{
    protected $interval = 10;

    public function actionIndex()
    {
        if ($this->checkIsRun()){
            return ExitCode::OK;
        }

        while (1){
            //重新设置db组件，获取需要执行的列表
            $config = \Yii::$app->getComponents()['db'];
            Yii::$app->set('db', $config);
            $list = TSyncTask::find()->where(['is_open' => 1])->andWhere(['<>','status',1])->all();

            foreach ($list as $obj){
                $IsExecuteHandler = new IsExecuteHandler();
                $CheckSourceHandler = new CheckSourceHandler();
                $OpenDbHandler = new OpenDbHandler();
                $CheckAidTablesHandler = new CheckAidTablesHandler();
                $EqualTablesHandler = new EqualTablesHandler();
                $ExecuteHandler = new ExecuteHandler();
                $CloseDbHandler = new CloseDbHandler();

                $IsExecuteHandler->setNextHandler($CheckSourceHandler);
                $CheckSourceHandler->setNextHandler($OpenDbHandler);
                $OpenDbHandler->setNextHandler($CheckAidTablesHandler);
                $CheckAidTablesHandler->setNextHandler($EqualTablesHandler);
                $EqualTablesHandler->setNextHandler($ExecuteHandler);
                $ExecuteHandler->setNextHandler($CloseDbHandler);


                $IsExecuteHandler->handlerSync($obj,null,null);
            }
            sleep($this->interval);
        }
    }

    private function checkIsRun(){
        exec("ps -ef |grep -v grep|grep -v 'sh -c' | grep sync/run/index",$op);

        return count($op)>1;
    }
}
