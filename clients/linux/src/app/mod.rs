use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::bg;
use crate::settings::Settings;
use crate::ui;

#[derive(Clone)]
pub struct App {
    pub core: Arc<lb::Core>,
    pub sync_lock: Arc<Mutex<()>>,
    pub settings: Arc<RwLock<Settings>>,
    pub titlebar: ui::Titlebar,
    pub window: gtk::ApplicationWindow,
    pub overlay: gtk::Overlay,
    pub onboard: ui::OnboardScreen,
    pub account: ui::AccountScreen,
    pub bg_state: bg::State,
}

mod imp_account_create;
mod imp_account_import;
mod imp_activate;
mod imp_close_file;
mod imp_delete_files;
mod imp_errors;
mod imp_export_files;
mod imp_file_copy_paste;
mod imp_import_files;
mod imp_new_file;
mod imp_open_file;
mod imp_rename_file;
mod imp_save_file;
mod imp_search;
mod imp_settings_dialog;
mod imp_sview_ctrl_click;
mod imp_sview_insert_files;
mod imp_sync_account;
mod imp_theme_stuff;
mod imp_tree_receive_drop;
mod imp_tree_toggle_col;
mod imp_update_sync_status;