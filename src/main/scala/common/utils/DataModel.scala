package common.utils

//Csv元数据-->Hive元数据表
case class T_HIVE_BATCH_ORIGINAL_TABLE_TMP(
                                            original_table_id: String,
                                            original_table_name: String,
                                            ddiinstance_urn: String,
                                            logic_storage_id: String,
                                            caseIdentifier: String,
                                            period_type: String,
                                            name_zh: String,
                                            source_system: String,
                                            file_id: String,
                                            col_num: Int,
                                            row_num: Int,
                                            batch_id: String
                                          )

//Csv元数据-->Hive元数据列
case class T_HIVE_BATCH_ORIGINAL_COULUMN_TMP(
                                              column_name: String,
                                              column_label: String,
                                              is_case_identifier: String,
                                              original_table_id: String
                                            )

//Hbase 全量表
case class T_HBASE_FULL_DEMENSION_TABLE(
                                         full_demension_table_id: String,
                                         full_demension_table_name: String,
                                         ddiinstance_urn: String,
                                         logic_storage_id: String,
                                         caseIdentifier: String,
                                         period_type: String,
                                         name_zh: String,
                                         col_num: Int,
                                         row_num: Int
                                       )

//Hbase 全量表-列
case class T_HBASE_FULL_DEMENSION_COLUMN(
                                          column_name: String,
                                          column_label: String,
                                          is_case_identifier: String,
                                          full_demension_table_id: String,
                                          original_column_name: String,
                                          comfirm_source_system: String,
                                          comfirm_update_frequency: String
                                        )

//Hbase 全量批次表
case class T_HBASE_BATCH_DEMENSION_TABLE(
                                          batch_demension_table_id: String,
                                          batch_demension_table_name: String,
                                          ddiinstance_urn: String,
                                          logic_storage_id: String,
                                          caseIdentifier: String,
                                          period_type: String,
                                          name_zh: String,
                                          col_num: Int,
                                          row_num: Int,
                                          batch_id:String
                                        )

//Hbase 全量确权表
case class T_FULL_CONFIRM_TABLE(
                                 full_comfirm_table_id: String,
                                 full_comfirm_table_name: String,
                                 ddiinstance_urn: String,
                                 logic_storage_id: String,
                                 caseIdentifier: String,
                                 period_type: String,
                                 name_zh: String,
                                 col_num: Int,
                                 row_num: Int,
                                 create_time: Long
                               )

//Hbase 全量确权表-列
case class T_FULL_CONFIRM_COLUMN(
                                  column_id: String,
                                  column_name: String,
                                  column_label: String,
                                  is_case_identifier: String,
                                  full_comfirm_table_id: String
                                )

//Hbase 全量批次确权表
case class T_BATCH_CONFIRM_TABLE(
                                  batch_confirm_table_id: String,
                                  batch_confirm_table_name: String,
                                  ddiinstance_urn: String,
                                  logic_storage_id: String,
                                  caseIdentifier: String,
                                  period_type: String,
                                  name_zh: String,
                                  col_num: Int,
                                  row_num: Int,
                                  create_time: Long,
                                  opration_type: String,
                                  batch_id: String
                                )

//Hbase 全量批次确权表-列
case class T_BATCH_CONFIRM_COLUMN(
                                   column_id: String,
                                   column_name: String,
                                   column_label: String,
                                   is_case_identifier: String,
                                   batch_confirm_table_id: String
                                 )
