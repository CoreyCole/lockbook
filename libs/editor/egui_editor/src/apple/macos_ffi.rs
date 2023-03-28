use crate::apple::keyboard::NSKeys;
use crate::WgpuEditor;
use egui::PointerButton::{Primary, Secondary};
use egui::{Event, Pos2, Vec2};
use std::ffi::{c_char, c_void, CStr};

/// (macos only)
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn key_event(
    obj: *mut c_void, key_code: u16, shift: bool, ctrl: bool, option: bool, command: bool,
    pressed: bool, characters: *const c_char,
) {
    let obj = &mut *(obj as *mut WgpuEditor);

    let modifiers = egui::Modifiers { alt: option, ctrl, shift, mac_cmd: command, command };

    obj.raw_input.modifiers = modifiers;

    let key = NSKeys::from(key_code).unwrap();

    let mut clip_event = false;
    if pressed && key == NSKeys::V && modifiers.command {
        let clip = obj.from_host.take().unwrap_or_default();
        obj.raw_input.events.push(Event::Text(clip));
        clip_event = true
    }

    // Event::Text
    if !clip_event && pressed && (modifiers.shift_only() || modifiers.is_none()) && key.valid_text()
    {
        let text = CStr::from_ptr(characters).to_str().unwrap().to_string();
        obj.raw_input.events.push(Event::Text(text));
    }

    // Event::Key
    if let Some(key) = key.egui_key() {
        obj.raw_input
            .events
            .push(Event::Key { key, pressed, modifiers });
    }
}

/// (macos only)
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn scroll_wheel(obj: *mut c_void, scroll_wheel: f32) {
    let obj = &mut *(obj as *mut WgpuEditor);
    obj.raw_input
        .events
        .push(Event::PointerMoved(Pos2::new(250.0, 250.0))); // todo remove?
    obj.raw_input
        .events
        .push(Event::Scroll(Vec2::new(0.0, scroll_wheel * 2.0)))
}

/// (macos only)
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn mouse_moved(obj: *mut c_void, x: f32, y: f32) {
    let obj = &mut *(obj as *mut WgpuEditor);
    obj.raw_input
        .events
        .push(Event::PointerMoved(Pos2 { x, y }))
}

/// (macos only)
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn mouse_button(
    obj: *mut c_void, x: f32, y: f32, pressed: bool, primary: bool,
) {
    let obj = &mut *(obj as *mut WgpuEditor);
    obj.raw_input.events.push(Event::PointerButton {
        pos: Pos2 { x, y },
        button: if primary { Primary } else { Secondary },
        pressed,
        modifiers: Default::default(),
    })
}